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
        },
        "dvf_change_detector": {
            "detect_changes": [],
            "trigger_reload": ["detect_changes"],
        },
    }
