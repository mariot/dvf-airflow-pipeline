"""Rent indicators ("Carte des loyers", ANIL / ministry of housing) -> PostgreSQL.

    sync_schema ──> plan_editions ──> load_edition (mapped: one per year)

* ``plan_editions``: the years to load are the DVF years already loaded
  (``dvf.v_load_status``), so rents and sales cover the same period. For each
  year it asks the data.gouv.fr API for the dataset
  ``carte-des-loyers-indicateurs-de-loyers-dannonce-par-commune-en-<year>``
  and picks its four indicator files. A year with no edition (there is none
  for 2021: the series goes 2018, then 2022 onwards) is logged and skipped.
* ``load_edition``: downloads the four files (skipped when the ETag is
  unchanged) and REPLACES that year in ``dvf.ref_loyers`` in one transaction.

What the data is (guide d'utilisation, December 2025)
    ``loypredm2`` is a PREDICTED advertised rent in EUR/m², charges included,
    for an unfurnished reference property (apartment 52 m², 1-2 rooms 37 m²,
    3+ rooms 72 m², house 92 m²) advertised in Q3 of the edition year. It is
    not a median nor a mean. Editions are NOT comparable over time: the
    grouping of communes (mailles) is rebuilt every year. Reliability is low
    when fewer than 30 ads were seen in the commune or when R² < 0.5.
    Mandatory credit: "Estimations ANIL, à partir des données du Groupe
    SeLoger et de leboncoin".

Format
    Unlike DVF: semicolon separator, decimal COMMA, Windows-1252 encoding.
    Columns are mapped by name; a value that stops parsing is NULLed and the
    format gate fails the load above DVF_MAX_REJECT_RATIO.
"""

import csv
import io
import logging
from collections.abc import Callable, Iterable, Iterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Param, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException
from dvf_io import (
    SCHEMA_ADVISORY_LOCK_ID,
    LoadStats,
    apply_sql_file,
    check_format,
    copy_buffer,
    download_file,
    to_code_commune,
    to_code_departement,
    to_decimal,
    to_int,
    to_text,
    ungzip,
)

from dvf_common import HTTP_TIMEOUT, LOYERS_DAG_ID, DvfConfig

log = logging.getLogger(__name__)

DATASET_API = "https://www.data.gouv.fr/api/1/datasets/{slug}/"
DATASET_SLUG = "carte-des-loyers-indicateurs-de-loyers-dannonce-par-commune-en-{year}"

# File name stem on data.gouv.fr -> segment stored in dvf.ref_loyers
SEGMENTS = {
    "pred-app-mef-dhup": "appartement",
    "pred-app12-mef-dhup": "appartement_1_2_pieces",
    "pred-app3-mef-dhup": "appartement_3_pieces_plus",
    "pred-mai-mef-dhup": "maison",
}
MIN_ROWS = 30_000  # one row per commune of France (~34 900)


def to_decimal_fr(value: str) -> Decimal | None:
    """'15,9931995588236' -> Decimal('15.9931995588236') (decimal comma)."""
    return to_decimal(value.strip().replace(",", "."))


def to_lower_text(value: str) -> str | None:
    text = to_text(value)
    return text.lower() if text else None


# Target column of dvf.ref_loyers -> (source column, converter)
LOYER_COLUMNS: dict[str, tuple[str, Callable[[str], Any]]] = {
    "code_insee": ("INSEE_C", to_code_commune),
    "nom_commune": ("LIBGEO", to_text),
    "dep_code": ("DEP", to_code_departement),
    "epci_code": ("EPCI", to_text),
    "id_maille": ("id_zone", to_text),
    "loyer_m2": ("loypredm2", to_decimal_fr),
    "loyer_m2_bas": ("lwr.IPm2", to_decimal_fr),
    "loyer_m2_haut": ("upr.IPm2", to_decimal_fr),
    "niveau_prediction": ("TYPPRED", to_lower_text),
    "nb_obs_commune": ("nbobs_com", to_int),
    "nb_obs_maille": ("nbobs_mail", to_int),
    "r2_ajuste": ("R2_adj", to_decimal_fr),
}
REQUIRED = {"code_insee", "dep_code", "loyer_m2"}


def decode(raw: bytes) -> str:
    """Bytes of a downloaded file -> text (gunzipped first: static.data.gouv.fr
    serves these .csv with Content-Encoding: gzip). The text is Windows-1252;
    UTF-8 is accepted too in case a future edition switches.
    """
    raw = ungzip(raw)
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode("cp1252")


def iter_loyers(lines: Iterable[str], stats: LoadStats) -> Iterator[list[Any]]:
    """Yield one typed row per commune, in LOYER_COLUMNS order."""
    reader = csv.DictReader(lines, delimiter=";")
    missing = [
        src for src, _ in LOYER_COLUMNS.values() if src not in (reader.fieldnames or [])
    ]
    if missing:
        raise AirflowFailException(f"Loyers schema drift: missing columns {missing}")
    for record in reader:
        stats.rows_read += 1
        row: list[Any] = []
        try:
            for target, (source, convert) in LOYER_COLUMNS.items():
                try:
                    value = convert(record[source] or "")
                except ValueError:
                    if target in REQUIRED:
                        raise
                    stats.nulled[target] += 1
                    value = None
                if value is None and target in REQUIRED:
                    raise ValueError(f"{target} is required")
                row.append(value)
        except ValueError as exc:
            stats.rows_rejected += 1
            if stats.rows_rejected <= 10:
                log.warning("Rejected rent row %r: %s", record.get("INSEE_C"), exc)
            continue
        stats.rows_loaded += 1
        yield row


def pick_files(resources: list[dict[str, Any]]) -> dict[str, str]:
    """{segment: url} from a dataset's resources, matched on the file name."""
    files: dict[str, str] = {}
    for resource in resources:
        stem = Path(resource.get("url", "")).name.removesuffix(".csv")
        if stem in SEGMENTS:
            files[SEGMENTS[stem]] = resource["url"]
    return files


def fetch_edition(session: requests.Session, year: int) -> dict[str, Any] | None:
    """The year's four indicator files, or None when data.gouv has no edition."""
    response = session.get(
        DATASET_API.format(slug=DATASET_SLUG.format(year=year)), timeout=HTTP_TIMEOUT
    )
    if response.status_code == 404:
        return None
    response.raise_for_status()
    files = pick_files(response.json().get("resources", []))
    missing = sorted(set(SEGMENTS.values()) - set(files))
    if missing:
        raise AirflowFailException(f"Loyers {year}: missing indicator files {missing}")
    return {"year": year, "files": files}


@dag(
    dag_id=LOYERS_DAG_ID,
    description="Load the 'Carte des loyers' rent indicators for the DVF years",
    # New editions come out in December; a monthly check is cheap (ETag skip).
    schedule="0 7 2 * *",
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    params={
        "years": Param(
            [],
            type="array",
            description="Years to load. Empty = the DVF years already loaded.",
        ),
    },
    tags=["data_processing", "dvf", "loyers", "postgres"],
    doc_md=__doc__,
)
def dvf_loyers_pipeline() -> None:
    @task(execution_timeout=timedelta(minutes=10))
    def sync_schema() -> None:
        """Apply init.sql (idempotent), which creates dvf.ref_loyers."""
        apply_sql_file(DvfConfig.from_airflow(), "init.sql")

    @task(execution_timeout=timedelta(minutes=5))
    def plan_editions() -> list[dict[str, Any]]:
        cfg = DvfConfig.from_airflow()
        years = [int(y) for y in get_current_context()["params"]["years"]]
        if not years:
            hook = PostgresHook(postgres_conn_id=cfg.conn_id)
            years = [
                int(r[0])
                for r in hook.get_records(
                    "SELECT annee FROM dvf.v_load_status ORDER BY annee"
                )
            ]
        if not years:
            raise AirflowFailException(
                "No DVF year loaded yet: nothing to align rents on"
            )
        editions = []
        with requests.Session() as session:
            for year in years:
                edition = fetch_edition(session, year)
                if edition is None:
                    log.warning("No 'Carte des loyers' edition for %s: skipped", year)
                    continue
                editions.append(edition)
        log.info("Rent editions to load: %s", [e["year"] for e in editions])
        return editions

    @task(execution_timeout=timedelta(minutes=30), max_active_tis_per_dag=1)
    def load_edition(edition: dict[str, Any]) -> dict[str, Any]:
        """Replace one year of dvf.ref_loyers (its four segments) atomically."""
        cfg = DvfConfig.from_airflow()
        year = int(edition["year"])
        columns = "annee, segment, " + ", ".join(LOYER_COLUMNS)
        loaded: dict[str, int] = {}
        hook = PostgresHook(postgres_conn_id=cfg.conn_id)
        conn = hook.get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_advisory_xact_lock(%s)", (SCHEMA_ADVISORY_LOCK_ID,)
                )
                cur.execute("DELETE FROM dvf.ref_loyers WHERE annee = %s", (year,))
                for segment, url in sorted(edition["files"].items()):
                    source = download_file(url, cfg.loyers_path(year, url))
                    stats = LoadStats()
                    text = decode(Path(source["path"]).read_bytes())
                    rows = iter_loyers(io.StringIO(text, newline=""), stats)
                    buffer = io.StringIO()
                    writer = csv.writer(buffer, lineterminator="\n")
                    writer.writerows([year, segment, *row] for row in rows)
                    copy_buffer(
                        cur,
                        f"COPY dvf.ref_loyers ({columns}) FROM STDIN WITH (FORMAT csv)",
                        buffer,
                    )
                    if stats.rows_loaded < MIN_ROWS:
                        raise AirflowFailException(
                            f"Loyers {year} {segment}: only {stats.rows_loaded:,} rows"
                        )
                    if stats.reject_ratio > cfg.max_reject_ratio:
                        raise AirflowFailException(
                            f"Loyers {year} {segment}: reject ratio "
                            f"{stats.reject_ratio:.4%} ({stats.rows_rejected:,} rows)"
                        )
                    check_format(stats, cfg.max_reject_ratio)
                    loaded[segment] = stats.rows_loaded
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()
        log.info("Rents %s loaded: %s", year, loaded)
        return {"year": year, "rows": loaded}

    editions = plan_editions()
    sync_schema() >> editions
    load_edition.expand(edition=editions)


dvf_loyers_pipeline()
