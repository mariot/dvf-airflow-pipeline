import dvf_zonages_pipeline as zon
import pytest
from airflow.sdk.exceptions import AirflowFailException

SQUARE = {
    "type": "Polygon",
    "coordinates": [[[-0.56, 44.84], [-0.55, 44.84], [-0.55, 44.85], [-0.56, 44.84]]],
}


def quartier(**props):
    base = {
        "gid": 279,
        "nom": "La Bastide",
        "insee": "33063",
        "valide": "True",
        "quarpoli": "True",
        "mdate": "2026-09-24T12:24:14+00:00",
    }
    return {"type": "Feature", "properties": base | props, "geometry": SQUARE}


def iris(code_iris="330630101", **props):
    base = {
        "code_insee": code_iris[:5],
        "nom_commune": "Bordeaux",
        "code_iris": code_iris,
        "nom_iris": "Le Lac 1",
        "type_iris": "A",
    }
    return {"type": "Feature", "properties": base | props, "geometry": SQUARE}


def test_quartier_rows_types_flags_and_keeps_geometry():
    rows, rejected = zon.quartier_rows(
        [quartier(), quartier(gid=280, valide="False", quarpoli=None)]
    )
    assert rejected == 0
    assert rows[0][:6] == [
        279,
        "33063",
        "La Bastide",
        True,
        True,
        "2026-09-24T12:24:14+00:00",
    ]
    assert rows[1][3:5] == [None, False]
    assert '"Polygon"' in rows[0][6]


@pytest.mark.parametrize(
    "bad",
    [
        quartier(insee="3306"),
        quartier(nom=" "),
        quartier(gid=None),
        {
            "properties": {"gid": 1, "nom": "X", "insee": "33063"},
            "geometry": {"type": "Point", "coordinates": [0, 0]},
        },
        {"properties": {"gid": 1, "nom": "X", "insee": "33063"}, "geometry": None},
    ],
)
def test_quartier_rows_rejects_incomplete_features(bad):
    rows, rejected = zon.quartier_rows([bad])
    assert (rows, rejected) == ([], 1)


def test_iris_rows_checks_codes_against_the_department():
    rows, rejected = zon.iris_rows(
        [
            iris(),
            iris("172000000"),
            iris("33063010"),
            iris("330630102", code_insee="33281"),
        ],
        "33",
    )
    assert [r[0] for r in rows] == ["330630101"]
    assert rows[0][1:5] == ["33063", "Bordeaux", "Le Lac 1", "A"]
    assert rejected == 3


def test_fetch_iris_pages_until_number_matched(monkeypatch):
    pages = {0: [iris("330630101"), iris("330630102")], 2: [iris("330630103")]}
    calls = []

    class Response:
        def __init__(self, start):
            self.start = start

        def raise_for_status(self):
            pass

        def json(self):
            return {"numberMatched": 3, "features": pages.get(self.start, [])}

    def get(url, timeout):
        start = int(url.split("start=")[1])
        calls.append(start)
        return Response(start)

    monkeypatch.setattr(zon.requests, "get", get)
    features = zon.fetch_iris("https://wfs?dep={dep}&start={start}", "33")
    assert [f["properties"]["code_iris"] for f in features] == [
        "330630101",
        "330630102",
        "330630103",
    ]
    assert calls == [0, 2]


def test_check_rejects_gates():
    zon.check_rejects("q", 75, 0, 20)
    with pytest.raises(AirflowFailException, match="only 10 rows"):
        zon.check_rejects("q", 10, 0, 20)
    with pytest.raises(AirflowFailException, match="rejected"):
        zon.check_rejects("q", 75, 5, 20)
