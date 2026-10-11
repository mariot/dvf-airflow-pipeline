import csv
import io
from collections import Counter
from decimal import Decimal

import pytest

import dvf_postgres_pipeline as dvf

HEADER = [c.name for c in dvf.COLUMNS]

# Real lines from geo-dvf 2024/full.csv.gz
SAMPLE = """\
2024-1,2024-01-02,000001,Vente,346.5,,,LE DELIVRE,B020,01230,01076,Chaley,01,,,010760000B0514,,,,,,,,,,,,,0,,,,,P,prés,,,99,5.530952,45.952439
2024-4,2024-01-03,000001,Vente,329500,29,,PL DU JURA,0500,01170,01173,Gex,01,,,01173000AI0551,,,175,,29,,,,,,,,2,3,Dépendance,,0,,,,,,6.058695,46.332212
"""


def _csv(header: list[str], body: str) -> list[str]:
    return [",".join(header) + "\n", *body.splitlines(keepends=True)]


# ---------------------------------------------------------------- converters
@pytest.mark.parametrize(
    ("raw", "expected"),
    [("", None), ("000001", 1), ("42", 42), ("3.0", 3), ("45.6", 46)],
)
def test_to_int(raw, expected):
    assert dvf.to_int(raw) == expected


def test_to_int_rejects_garbage():
    with pytest.raises(ValueError):
        dvf.to_int("abc")
    with pytest.raises(ValueError):
        dvf.to_int("nan")


def test_to_decimal_is_exact():
    assert dvf.to_decimal("346.5") == Decimal("346.5")
    assert dvf.to_decimal("") is None
    with pytest.raises(ValueError):
        dvf.to_decimal("12,5")


def test_coordinates_are_range_checked():
    assert dvf.to_latitude("45.952439") == pytest.approx(45.952439)
    with pytest.raises(ValueError):
        dvf.to_latitude("123.0")
    with pytest.raises(ValueError):
        dvf.to_longitude("-200")


@pytest.mark.parametrize("code", ["01076", "75056", "2A004", "2B033", "97411"])
def test_code_commune_accepts_insee_codes(code):
    assert dvf.to_code_commune(code) == code


@pytest.mark.parametrize("code", ["1076", "2C004", "ABCDE"])
def test_code_commune_rejects_invalid(code):
    with pytest.raises(ValueError):
        dvf.to_code_commune(code)


# ------------------------------------------------------------ row cleaning
def test_iter_clean_rows_types_and_tracking_keys():
    stats = dvf.LoadStats()
    rows = list(dvf.iter_clean_rows(_csv(HEADER, SAMPLE), 2024, stats))

    assert stats.rows_read == 2 and stats.rows_rejected == 0
    first = dict(zip(dvf.TARGET_COLUMNS, rows[0], strict=True))
    assert first["annee"] == 2024
    assert first["num_ligne_source"] == 1
    assert first["numero_disposition"] == 1
    assert first["valeur_fonciere"] == Decimal("346.5")
    assert first["code_postal"] == "01230"  # leading zero preserved
    assert first["adresse_numero"] is None
    assert first["surface_terrain"] == 99
    assert rows[1][1] == 2


def test_invalid_required_field_rejects_row_but_optional_is_nulled():
    body = SAMPLE.replace("01076", "XXXXX", 1).replace("329500", "n/a", 1)
    stats = dvf.LoadStats()
    rows = list(dvf.iter_clean_rows(_csv(HEADER, body), 2024, stats))

    assert stats.rows_rejected == 1  # bad code_commune
    assert len(rows) == 1
    assert rows[0][TARGET_INDEX["valeur_fonciere"]] is None  # nulled, not rejected
    assert stats.nulled == Counter({"valeur_fonciere": 1})


def test_wrong_field_count_is_rejected():
    stats = dvf.LoadStats()
    list(dvf.iter_clean_rows(_csv(HEADER, "2024-9,2024-01-01,1\n"), 2024, stats))
    assert stats.rows_rejected == 1


def test_columns_are_mapped_by_name_not_position():
    reordered = list(reversed(HEADER))
    body = "".join(
        ",".join(reversed(next(csv.reader([line])))) + "\n"
        for line in SAMPLE.splitlines()
    )
    rows = list(dvf.iter_clean_rows(_csv(reordered, body), 2024, dvf.LoadStats()))
    expected = list(dvf.iter_clean_rows(_csv(HEADER, SAMPLE), 2024, dvf.LoadStats()))
    assert rows == expected


def test_schema_drift_fails_fast():
    header = [h for h in HEADER if h != "valeur_fonciere"]
    with pytest.raises(dvf.AirflowFailException, match="valeur_fonciere"):
        list(dvf.iter_clean_rows(_csv(header, ""), 2024, dvf.LoadStats()))


TARGET_INDEX = {name: i for i, name in enumerate(dvf.TARGET_COLUMNS)}


# ---------------------------------------------------------------- format
def test_raw_dgfip_pipe_file_fails_schema_check():
    """The raw DGFiP export is pipe-separated with French headers: fail fast."""
    lines = [
        "Identifiant de document|No disposition|Date mutation|Nature mutation|Valeur fonciere\n",
        "|000001|02/01/2024|Vente|346,50\n",
    ]
    with pytest.raises(dvf.AirflowFailException, match="schema drift"):
        list(dvf.iter_clean_rows(lines, 2024, dvf.LoadStats()))


def test_decimal_comma_prices_fail_the_load_instead_of_being_nulled():
    body = SAMPLE.replace(",Vente,346.5,", ',Vente,"346,5",').replace(
        ",Vente,329500,", ',Vente,"329500,00",'
    )
    stats = dvf.LoadStats()
    rows = list(dvf.iter_clean_rows(_csv(HEADER, body), 2024, stats))
    assert len(rows) == 2  # rows survive, the price is NULLed...
    assert stats.nulled["valeur_fonciere"] == 2
    with pytest.raises(dvf.AirflowFailException, match="valeur_fonciere=100.00%"):
        dvf.check_format(stats, max_ratio=0.001)  # ...but the load is refused


def test_clean_file_passes_format_check():
    stats = dvf.LoadStats()
    list(dvf.iter_clean_rows(_csv(HEADER, SAMPLE), 2024, stats))
    dvf.check_format(stats, max_ratio=0.001)


def test_prices_keep_their_decimals():
    rows = list(dvf.iter_clean_rows(_csv(HEADER, SAMPLE), 2024, dvf.LoadStats()))
    assert rows[0][TARGET_INDEX["valeur_fonciere"]] == Decimal("346.5")


# --------------------------------------------------------------- history
@pytest.mark.parametrize(
    ("loaded", "previous"),
    [(100, None), (100, 100), (150, 100), (81, 100)],
)
def test_history_gate_allows_first_load_growth_and_small_revisions(loaded, previous):
    dvf.check_history(loaded, previous, max_shrink=0.2)


def test_history_gate_refuses_a_shrunken_year():
    with pytest.raises(dvf.AirflowFailException, match="shrink"):
        dvf.check_history(79, 100, max_shrink=0.2)


def test_parse_http_date():
    parsed = dvf.parse_http_date("Mon, 18 May 2026 12:56:30 GMT")
    assert parsed.isoformat() == "2026-05-18T12:56:30+00:00"
    assert dvf.parse_http_date(None) is None
    assert dvf.parse_http_date("garbage") is None


# ------------------------------------------------- driver-agnostic COPY
class Psycopg2Cursor:
    def __init__(self):
        self.payloads: list[str] = []

    def copy_expert(self, sql, file):
        self.payloads.append(file.read())


class Psycopg3Cursor:
    class _Copy:
        def __init__(self, sink):
            self.sink = sink

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def write(self, data):
            self.sink[-1] += data

    def __init__(self):
        self.payloads: list[str] = []

    def copy(self, sql):
        self.payloads.append("")
        return self._Copy(self.payloads)


@pytest.mark.parametrize("cursor_cls", [Psycopg2Cursor, Psycopg3Cursor])
def test_stream_rows_chunks_and_serializes_nulls(cursor_cls):
    cursor = cursor_cls()
    stats = dvf.LoadStats()
    rows = [[2024, i, "a,b", None, Decimal("1.50")] for i in range(1, 6)]

    dvf.stream_rows_into_postgres(cursor, rows, "dvf.t", chunk_rows=2, stats=stats)

    assert stats.rows_loaded == 5
    assert stats.chunks == 3  # 2 + 2 + 1
    parsed = [r for p in cursor.payloads for r in csv.reader(io.StringIO(p))]
    assert parsed[0] == ["2024", "1", "a,b", "", "1.50"]  # quoted comma, NULL as empty


# -------------------------------------------------------------- DAG integrity
_PARSE_DAGS_FOLDER = """
import json, sys
from airflow.dag_processing.dagbag import DagBag
bag = DagBag(dag_folder=sys.argv[1])
print(json.dumps({
    "import_errors": {k: str(v) for k, v in bag.import_errors.items()},
    "dags": {
        d.dag_id: {t.task_id: sorted(t.upstream_task_ids) for t in d.tasks}
        for d in bag.dags.values()
    },
}))
"""


def test_dags_folder_parses_like_the_dag_processor():
    """Parse dags/ in a FRESH interpreter, like the dag-processor does.

    Running in-process would hide cross-file import problems (such as a DAG
    registered twice), because pytest has already imported the modules.
    """
    import json
    import os
    import subprocess
    import sys
    from pathlib import Path

    dags_folder = Path(dvf.__file__).parent
    result = subprocess.run(
        [sys.executable, "-c", _PARSE_DAGS_FOLDER, str(dags_folder)],
        capture_output=True,
        text=True,
        check=True,
        env={**os.environ, "PYTHONPATH": str(dags_folder)},
    )
    parsed = json.loads(result.stdout.strip().splitlines()[-1])

    assert parsed["import_errors"] == {}
    assert parsed["dags"] == {
        "dvf_postgres_pipeline": {
            "download_dvf_file": [],
            "sync_schema": [],
            "load_dvf_into_postgres": ["download_dvf_file", "sync_schema"],
            "load_communes_reference": ["sync_schema"],
            "refresh_semantic_layer": [
                "load_communes_reference",
                "load_dvf_into_postgres",
            ],
        },
        "dvf_logement_pipeline": {
            "sync_schema": [],
            "plan_rpls": ["sync_schema"],
            "load_rpls": ["plan_rpls"],
            "load_lovac": ["sync_schema"],
            "load_series_longues": ["sync_schema"],
            "load_cdc": ["sync_schema"],
        },
        "dvf_loyers_pipeline": {
            "sync_schema": [],
            "plan_editions": ["sync_schema"],
            "load_edition": ["plan_editions"],
        },
        "dvf_change_detector": {
            "detect_changes": [],
            "trigger_reload": ["detect_changes"],
        },
    }


# ------------------------------------------------------------- communes list
COMMUNES_HEADER = (
    "code_insee,nom_standard,nom_sans_accent,typecom,dep_code,epci_code,epci_nom,"
    "code_postal,codes_postaux,population,superficie_km2,densite,latitude_centre,"
    "longitude_centre,grille_densite_texte\n"
)
COMMUNES_SAMPLE = (
    "33063,Bordeaux,bordeaux,COM,33,243300316,Bordeaux Métropole,33000,"
    '"33000, 33001, 33100",267991,50,5397.2001953125,44.857,-0.574,Grands centres urbains\n'
    "2A004,Ajaccio,ajaccio,COM,2A,242010056,CA du Pays Ajaccien,20000,20000,"
    "75000,82.03,914.3,41.93,8.74,Centres urbains intermédiaires\n"
)


def test_iter_communes_types_and_keeps_corsica_codes():
    stats = dvf.LoadStats()
    rows = list(
        dvf.iter_communes([COMMUNES_HEADER, *COMMUNES_SAMPLE.splitlines(True)], stats)
    )
    assert [r[0] for r in rows] == ["33063", "2A004"]
    bordeaux = dict(zip(dvf.COMMUNE_COLUMNS, rows[0]))
    assert bordeaux["nom"] == "Bordeaux"
    assert bordeaux["epci_nom"] == "Bordeaux Métropole"
    assert bordeaux["population"] == 267991
    assert bordeaux["densite"] == Decimal("5397.2001953125")
    assert (bordeaux["latitude"], bordeaux["longitude"]) == (44.857, -0.574)
    assert stats.rows_rejected == 0


def test_iter_communes_rejects_a_row_without_a_valid_code():
    stats = dvf.LoadStats()
    bad = "3306,Nowhere,nowhere,COM,33,,,,,,,,,,\n"
    rows = list(
        dvf.iter_communes(
            [COMMUNES_HEADER, bad, *COMMUNES_SAMPLE.splitlines(True)], stats
        )
    )
    assert len(rows) == 2 and stats.rows_rejected == 1


def test_iter_communes_schema_drift_fails_fast():
    header = COMMUNES_HEADER.replace("nom_standard", "nom")
    with pytest.raises(dvf.AirflowFailException, match="nom_standard"):
        list(dvf.iter_communes([header], dvf.LoadStats()))


def test_millesime_from_url():
    assert (
        dvf.millesime_from_url("https://x/20260617/communes-france-2026.csv.gz") == 2026
    )
    assert dvf.millesime_from_url("https://x/other.csv.gz") is None
