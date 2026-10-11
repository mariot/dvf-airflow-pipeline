"""Housing reference data next to DVF: social housing, vacancy, long series, costs.

    sync_schema ──┬─> plan_rpls ──> load_rpls (mapped: one per edition)
                  ├─> load_lovac
                  ├─> load_series_longues
                  └─> load_cdc

Each source REPLACES its own table (or its own edition) in one transaction,
with the same gates as DVF: minimum row count, reject ratio, format drift.
None of them is part of the semantic layer yet.

Sources
  * RPLS: "répertoire des logements locatifs des bailleurs sociaux" (SDES),
    one row per social dwelling at 1 January of each edition, from the DiDo
    API. Filtered to DVF_RPLS_DEPARTEMENTS (default '33': the national file
    is ~5.4 M rows per edition). Editions aligned on the DVF years; there is
    none before 2022. Columns are renamed across editions (DEPCOM vs
    DEPCOM_CODE, IRIS 2022 vs 2024): mapped through aliases.
  * LOVAC: vacant private dwellings (DGFiP 1767 bis Com + Fichiers fonciers,
    Cerema), communes / departments / regions / France, editions 2020-2026.
    Wide files (one column per edition) are unpivoted. "s" = statistical
    secret: stored as NULL with a flag, never as 0, so commune values don't
    add up to the department totals (load those from the department level).
    Edition N mostly describes 1 January N-1. Method break in 2025 (sources
    taken before the tax processing chain): France drops from 3.69 M to
    2.38 M vacant dwellings. No private stock is published for edition 2026.
  * Long series 1800-2020 (IGEDD, J. Friggit): housing prices, rents,
    construction costs, macro and financial series for France, the US and
    the UK, one row per (series, year). Amounts are in euros even before
    1999 (1 EUR = 6.55957 FRF); the latest values may be provisional.
  * CDC: median surfaces and cost prices of the social housing operations
    financed by the Caisse des Dépôts, by REGION and signing year (2018+).

Formats: RPLS and CDC are ';' with decimal points; LOVAC is ';', Windows-1252,
with thousands separators (' 26 755 ') in the aggregates; the long series are
a legacy .xls workbook (read with xlrd).
"""

import csv
import io
import logging
import re
from collections.abc import Callable, Iterable, Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task
from airflow.sdk.exceptions import AirflowFailException
from dvf_io import (
    SCHEMA_ADVISORY_LOCK_ID,
    LoadStats,
    apply_sql_file,
    check_format,
    copy_buffer,
    download_file,
    read_downloaded,
    to_code_commune,
    to_decimal,
    to_int,
    to_text,
)

from dvf_common import HTTP_TIMEOUT, LOGEMENT_DAG_ID, DvfConfig

log = logging.getLogger(__name__)

RPLS_CSV = (
    "https://data.statistiques.developpement-durable.gouv.fr/dido/api/v1/datafiles/"
    "f3c2f2cb-8fb1-40fd-8733-964247744c9a/csv"
)
DATAGOUV_DATASET = "https://www.data.gouv.fr/api/1/datasets/{slug}/"
CDC_CSV = (
    "https://opendata.caissedesdepots.fr/api/explore/v2.1/catalog/datasets/"
    "constructionrehabilitation_logementsocial_surface_prix/exports/csv"
)


# =============================================================================
# Shared parsing
# =============================================================================
def decode(raw: bytes) -> str:
    """UTF-8 (with or without BOM), else Windows-1252."""
    try:
        return raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        return raw.decode("cp1252")


def to_count(value: str) -> int | None:
    """' 26 755    ' -> 26755 (spaces, including non-breaking, are thousands separators)."""
    return to_int(re.sub(r"[\s  ]", "", value or ""))


def to_year(value: str) -> int | None:
    year = to_int(value)
    if year is not None and not 1000 <= year <= 2100:
        raise ValueError(f"implausible year {value!r}")
    return year


Spec = dict[str, tuple[tuple[str, ...], Callable[[str], Any]]]


def iter_typed(
    records: Iterable[dict[str, str]],
    fieldnames: list[str],
    spec: Spec,
    required: set[str],
    stats: LoadStats,
    label: str,
) -> Iterator[list[Any]]:
    """Typed rows from dict records; each target column accepts several source names."""
    names = {name.strip(): name for name in fieldnames}
    plan: list[tuple[str, str | None, Callable[[str], Any]]] = []
    for target, (aliases, convert) in spec.items():
        source = next((names[a] for a in aliases if a in names), None)
        if source is None and target in required:
            raise AirflowFailException(
                f"{label}: schema drift, none of {aliases} found"
            )
        plan.append((target, source, convert))
    for record in records:
        stats.rows_read += 1
        row: list[Any] = []
        try:
            for target, source, convert in plan:
                raw = (record.get(source) or "").strip() if source else ""
                try:
                    value = convert(raw)
                except ValueError:
                    if target in required:
                        raise
                    stats.nulled[target] += 1
                    value = None
                if value is None and target in required:
                    raise ValueError(f"{target} is required")
                row.append(value)
        except ValueError as exc:
            stats.rows_rejected += 1
            if stats.rows_rejected <= 10:
                log.warning("%s: rejected row: %s", label, exc)
            continue
        stats.rows_loaded += 1
        yield row


def check_gates(stats: LoadStats, min_rows: int, max_ratio: float, label: str) -> None:
    if stats.rows_loaded < min_rows:
        raise AirflowFailException(
            f"{label}: only {stats.rows_loaded:,} rows (expected >= {min_rows:,})"
        )
    if stats.reject_ratio > max_ratio:
        raise AirflowFailException(
            f"{label}: reject ratio {stats.reject_ratio:.4%} "
            f"({stats.rows_rejected:,} / {stats.rows_read:,})"
        )
    check_format(stats, max_ratio)


def replace_rows(
    cfg: DvfConfig,
    table: str,
    columns: list[str],
    rows: Iterable[list[Any]],
    where: str = "",
    params: tuple = (),
) -> None:
    """DELETE (all, or `where`) + COPY, in one transaction under the schema lock."""
    buffer = io.StringIO()
    csv.writer(buffer, lineterminator="\n").writerows(rows)
    hook = PostgresHook(postgres_conn_id=cfg.conn_id)
    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(%s)", (SCHEMA_ADVISORY_LOCK_ID,))
            cur.execute(f"DELETE FROM {table} {where}", params)
            copy_buffer(
                cur,
                f"COPY {table} ({', '.join(columns)}) FROM STDIN WITH (FORMAT csv)",
                buffer,
            )
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()


# =============================================================================
# RPLS
# =============================================================================
RPLS_SPEC: Spec = {
    "code_commune": (("DEPCOM", "DEPCOM_CODE"), to_code_commune),
    "nom_commune": (("LIBCOM", "DEPCOM_LIBELLE"), to_text),
    "code_postal": (("CODEPOSTAL",), to_text),
    "dep_code": (("DEP_CODE",), to_text),
    "epci_code": (("EPCI_CODE",), to_text),
    "code_iris": (("PLG_IRIS2024_CODE", "PLG_IRIS2022_CODE"), to_text),
    "qpv": (("QPV_LIBELLE",), to_text),
    "type_construction": (("TYPECONST_LIBELLE",), to_text),
    "nb_pieces": (("NBPIECE",), to_int),
    "surface_habitable": (("SURFHAB",), to_decimal),
    "annee_construction": (("CONSTRUCT",), to_year),
    "annee_mise_en_location": (("LOCAT",), to_year),
    "financement": (("FINAN_LIBELLE",), to_text),
    "droit": (("DROIT_LIBELLE",), to_text),
    "dpe_energie": (("DPEENERGIE",), to_text),
    "dpe_ges": (("DPESERRE",), to_text),
    "epsg": (("EPSG",), to_text),
    "x": (("X",), to_decimal),
    "y": (("Y",), to_decimal),
}
RPLS_REQUIRED = {"code_commune"}
RPLS_MIN_ROWS = 1_000  # Gironde alone holds ~117 000 social dwellings


def rpls_url(millesime: int, departement: str) -> str:
    return (
        f"{RPLS_CSV}?millesime={millesime}-01&withColumnName=true"
        "&withColumnDescription=false&withColumnUnit=false"
        f"&DEP_CODE=eq:{departement}"
    )


def iter_rpls(text: str, stats: LoadStats) -> Iterator[list[Any]]:
    reader = csv.DictReader(io.StringIO(text, newline=""), delimiter=";")
    yield from iter_typed(
        reader, reader.fieldnames or [], RPLS_SPEC, RPLS_REQUIRED, stats, "RPLS"
    )


# =============================================================================
# LOVAC
# =============================================================================
LOVAC_LEVELS = {  # resource title on data.gouv -> (niveau, code column, label column)
    "Lovac_opendata_Communes.csv": ("commune", "CODGEO_", "LIBGEO_"),
    "Lovac_opendata_Departements.csv": ("departement", "DEP", "LIB_DEP"),
    "Lovac_opendata_Regions.csv": ("region", "REG", "LIB_REG"),
}
LOVAC_MEASURES = {  # column prefix -> target
    "ff_pp_total_": "parc_prive",
    "pp_vacant_": "vacants",
    "pp_vacant_plus_2ans_": "vacants_plus_2ans",
    "pp_vacant_longue_duree_": "vacants_plus_2ans",  # same measure, renamed in 2022 (regions)
}
LOVAC_COLUMNS = [
    "millesime",
    "niveau",
    "code",
    "libelle",
    "parc_prive",
    "vacants",
    "vacants_secret",
    "vacants_plus_2ans",
    "vacants_plus_2ans_secret",
]


def lovac_value(raw: str) -> tuple[int | None, bool]:
    """('12 070', False) or (None, True) for the statistical secret 's'."""
    raw = (raw or "").strip()
    if raw.lower() == "s":
        return None, True
    return to_count(raw), False


def iter_lovac_wide(
    text: str, niveau: str, code_col: str, label_col: str, stats: LoadStats
) -> Iterator[list[Any]]:
    """Unpivot one wide LOVAC file (one column per edition) to one row per edition."""
    reader = csv.reader(io.StringIO(text, newline=""), delimiter=";")
    header = [h.strip() for h in next(reader)]
    find = lambda prefix: next(
        (i for i, h in enumerate(header) if h.startswith(prefix)), None
    )
    code_i, label_i = find(code_col), find(label_col)
    if code_i is None:
        raise AirflowFailException(
            f"LOVAC {niveau}: no {code_col} column in {header[:5]}"
        )
    by_edition: dict[int, dict[str, int]] = {}
    for i, name in enumerate(header):
        # longest prefix first: pp_vacant_plus_2ans_ also starts with pp_vacant_
        for prefix in sorted(LOVAC_MEASURES, key=len, reverse=True):
            match = re.fullmatch(re.escape(prefix) + r"(\d{2})", name)
            if match:
                by_edition.setdefault(2000 + int(match.group(1)), {})[
                    LOVAC_MEASURES[prefix]
                ] = i
                break
    if not by_edition:
        raise AirflowFailException(f"LOVAC {niveau}: no edition columns found")
    for record in reader:
        if not any(cell.strip() for cell in record):
            continue
        code = record[code_i].strip()
        if niveau == "commune":
            try:
                code = to_code_commune(code)
            except ValueError:
                stats.rows_read += 1
                stats.rows_rejected += 1
                continue
        label = record[label_i].strip() if label_i is not None else None
        for edition, cols in sorted(by_edition.items()):
            stats.rows_read += 1
            try:
                parc, _ = (
                    lovac_value(record[cols["parc_prive"]])
                    if "parc_prive" in cols
                    else (None, False)
                )
                vac, vac_s = (
                    lovac_value(record[cols["vacants"]])
                    if "vacants" in cols
                    else (None, False)
                )
                v2, v2_s = (
                    lovac_value(record[cols["vacants_plus_2ans"]])
                    if "vacants_plus_2ans" in cols
                    else (None, False)
                )
            except ValueError:
                stats.nulled["lovac_value"] += 1
                continue
            stats.rows_loaded += 1
            yield [edition, niveau, code, label, parc, vac, vac_s, v2, v2_s]


def iter_lovac_france(text: str, stats: LoadStats) -> Iterator[list[Any]]:
    """The France file is already long: one row per edition ('Millésime')."""
    reader = csv.DictReader(io.StringIO(text, newline=""), delimiter=";")
    for record in reader:
        record = {k.strip(): v for k, v in record.items() if k}
        stats.rows_read += 1
        edition = to_int((record.get("Millésime") or "").strip())
        if edition is None:
            stats.rows_rejected += 1
            continue
        parc, _ = lovac_value(record.get("ff_pp_total", ""))
        vac, vac_s = lovac_value(record.get("pp_vacant", ""))
        v2, v2_s = lovac_value(record.get("pp_vacant_plus_2ans", ""))
        stats.rows_loaded += 1
        yield [edition, "france", "FR", "France", parc, vac, vac_s, v2, v2_s]


def lovac_files(resources: list[dict[str, Any]]) -> dict[str, str]:
    """{resource title: url} for the four CSV levels of the LOVAC dataset."""
    wanted = set(LOVAC_LEVELS) | {"Lovac_opendata_France.csv"}
    files = {r["title"]: r["url"] for r in resources if r.get("title") in wanted}
    missing = sorted(wanted - set(files))
    if missing:
        raise AirflowFailException(f"LOVAC: missing resources {missing}")
    return files


# =============================================================================
# Long series 1800-2020 (xls)
# =============================================================================
SERIES_COLUMNS = ["pays", "categorie", "serie", "serie_en", "source", "annee", "valeur"]


def iter_series_longues(raw: bytes, stats: LoadStats) -> Iterator[list[Any]]:
    """Unpivot the 'Series' sheet: one row per (country, series, year).

    Layout: a header row 'Série | Series | | 1800 ... 2020'; section rows
    'A. France' (country) and 'A.a. Séries macroéconomiques' (category); then
    one row per series: French label, English label, source link, values.
    """
    import xlrd  # only this task needs it (docker/airflow/Dockerfile)

    sheet = xlrd.open_workbook(file_contents=raw).sheet_by_name("Series")
    header_row = next(
        (
            r
            for r in range(sheet.nrows)
            if str(sheet.cell_value(r, 0)).strip() == "Série"
        ),
        None,
    )
    if header_row is None:
        raise AirflowFailException("Long series: no 'Série' header row")
    years = {
        c: int(sheet.cell_value(header_row, c))
        for c in range(3, sheet.ncols)
        if isinstance(sheet.cell_value(header_row, c), float)
    }
    if len(years) < 200:
        raise AirflowFailException(f"Long series: only {len(years)} year columns")
    pays = categorie = None
    for r in range(header_row + 1, sheet.nrows):
        label = str(sheet.cell_value(r, 0)).strip()
        values = {c: sheet.cell_value(r, c) for c in years}
        numbers = {c: v for c, v in values.items() if isinstance(v, float)}
        if not numbers:
            if re.match(r"^[A-Z]\.\s", label):  # 'A. France'
                pays, categorie = label.split(".", 1)[1].strip(), None
            elif re.match(r"^[A-Z]\.[a-z]\.\s", label):  # 'A.a. Séries ...'
                categorie = label.split(".", 2)[2].strip()
            continue
        english = str(sheet.cell_value(r, 1)).strip() or None
        source = str(sheet.cell_value(r, 2)).strip() or None
        for c, value in numbers.items():
            stats.rows_read += 1
            stats.rows_loaded += 1
            yield [pays, categorie, label, english, source, years[c], value]


# =============================================================================
# CDC
# =============================================================================
CDC_SPEC: Spec = {
    "code_region": (("code_region",), to_text),
    "region": (("region",), to_text),
    "annee": (("annee_signature",), to_year),
    "construction_surface_m2": (
        ("construction_surface_mediane_des_operations_m2_su",),
        to_decimal,
    ),
    "construction_cout_m2": (
        ("construction_prix_de_revient_median_au_m2_des_operations",),
        to_decimal,
    ),
    "construction_cout_logement": (
        ("construction_prix_de_revient_median_des_operations_au_logement",),
        to_decimal,
    ),
    "rehabilitation_surface_m2": (
        ("rehabilitation_surface_mediane_des_operations_m2_su",),
        to_decimal,
    ),
    "rehabilitation_cout_m2": (
        ("rehabilitation_prix_de_revient_median_au_m2_des_operations",),
        to_decimal,
    ),
    "rehabilitation_cout_logement": (
        ("rehabilitation_prix_de_revient_median_des_operations_au_logement",),
        to_decimal,
    ),
}


def iter_cdc(text: str, stats: LoadStats) -> Iterator[list[Any]]:
    csv.field_size_limit(10_000_000)  # the geometry columns hold whole region polygons
    reader = csv.DictReader(io.StringIO(text, newline=""), delimiter=";")
    yield from iter_typed(
        reader,
        reader.fieldnames or [],
        CDC_SPEC,
        {"code_region", "annee"},
        stats,
        "CDC",
    )


# =============================================================================
# DAG
# =============================================================================
@dag(
    dag_id=LOGEMENT_DAG_ID,
    description="Load housing reference data: RPLS, LOVAC, long series, CDC costs",
    schedule="0 7 3 * *",  # monthly: every source is annual, re-checks are cheap
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["data_processing", "dvf", "logement", "postgres"],
    doc_md=__doc__,
)
def dvf_logement_pipeline() -> None:
    @task(execution_timeout=timedelta(minutes=10))
    def sync_schema() -> None:
        apply_sql_file(DvfConfig.from_airflow(), "init.sql")

    @task(execution_timeout=timedelta(minutes=5))
    def plan_rpls() -> list[int]:
        """RPLS editions for the DVF years (1 January of the year); missing ones skipped."""
        cfg = DvfConfig.from_airflow()
        hook = PostgresHook(postgres_conn_id=cfg.conn_id)
        years = [
            int(r[0])
            for r in hook.get_records(
                "SELECT annee FROM dvf.v_load_status ORDER BY annee"
            )
        ]
        editions = []
        with requests.Session() as session:
            for year in years:
                probe = session.get(
                    rpls_url(year, cfg.rpls_departements[0]),
                    stream=True,
                    timeout=HTTP_TIMEOUT,
                )
                probe.close()
                if probe.status_code == 404:
                    log.warning("No RPLS edition at 1 January %s: skipped", year)
                    continue
                probe.raise_for_status()
                editions.append(year)
        log.info("RPLS editions to load: %s", editions)
        return editions

    @task(execution_timeout=timedelta(minutes=30), max_active_tis_per_dag=1)
    def load_rpls(millesime: int) -> dict[str, Any]:
        cfg = DvfConfig.from_airflow()
        stats = LoadStats()
        rows: list[list[Any]] = []
        for departement in cfg.rpls_departements:
            path = cfg.reference_path("rpls", f"rpls-{millesime}-dep{departement}.csv")
            source = download_file(rpls_url(millesime, departement), path)
            text = decode(read_downloaded(source["path"]))
            rows.extend(
                [millesime, n, *row] for n, row in enumerate(iter_rpls(text, stats), 1)
            )
        check_gates(stats, RPLS_MIN_ROWS, cfg.max_reject_ratio, f"RPLS {millesime}")
        replace_rows(
            cfg,
            "dvf.ref_rpls",
            ["millesime", "num_ligne", *RPLS_SPEC],
            rows,
            "WHERE millesime = %s",
            (millesime,),
        )
        return {"millesime": millesime, "rows": stats.rows_loaded}

    @task(execution_timeout=timedelta(minutes=20))
    def load_lovac() -> dict[str, Any]:
        cfg = DvfConfig.from_airflow()
        dataset = requests.get(
            DATAGOUV_DATASET.format(slug=cfg.lovac_dataset), timeout=HTTP_TIMEOUT
        )
        dataset.raise_for_status()
        files = lovac_files(dataset.json().get("resources", []))
        stats = LoadStats()
        rows: list[list[Any]] = []
        for title, url in sorted(files.items()):
            source = download_file(url, cfg.reference_path("lovac", Path(url).name))
            text = decode(read_downloaded(source["path"]))
            if title in LOVAC_LEVELS:
                rows.extend(iter_lovac_wide(text, *LOVAC_LEVELS[title], stats))
            else:
                rows.extend(iter_lovac_france(text, stats))
        check_gates(stats, 30_000, cfg.max_reject_ratio, "LOVAC")
        replace_rows(cfg, "dvf.ref_lovac", LOVAC_COLUMNS, rows)
        return {"rows": stats.rows_loaded}

    @task(execution_timeout=timedelta(minutes=10))
    def load_series_longues() -> dict[str, Any]:
        cfg = DvfConfig.from_airflow()
        source = download_file(
            cfg.series_longues_url,
            cfg.reference_path("series", Path(cfg.series_longues_url).name),
        )
        stats = LoadStats()
        rows = list(iter_series_longues(read_downloaded(source["path"]), stats))
        check_gates(stats, 5_000, cfg.max_reject_ratio, "Long series")
        replace_rows(cfg, "dvf.ref_series_longues", SERIES_COLUMNS, rows)
        return {"rows": stats.rows_loaded}

    @task(execution_timeout=timedelta(minutes=10))
    def load_cdc() -> dict[str, Any]:
        cfg = DvfConfig.from_airflow()
        source = download_file(
            CDC_CSV, cfg.reference_path("cdc", "cdc-logement-social.csv")
        )
        stats = LoadStats()
        rows = list(iter_cdc(decode(read_downloaded(source["path"])), stats))
        check_gates(stats, 100, cfg.max_reject_ratio, "CDC")
        replace_rows(cfg, "dvf.ref_cdc_logement_social", list(CDC_SPEC), rows)
        return {"rows": stats.rows_loaded}

    schema = sync_schema()
    editions = plan_rpls()
    schema >> [editions, load_lovac(), load_series_longues(), load_cdc()]
    load_rpls.expand(millesime=editions)


dvf_logement_pipeline()
