import gzip
import io
from decimal import Decimal

import dvf_loyers_pipeline as loyers
import pytest
from dvf_io import LoadStats

# Real lines of pred-app-mef-dhup.csv (2025 edition), Windows-1252, CRLF.
SAMPLE = (
    '"id_zone";"INSEE_C";"LIBGEO";"EPCI";"DEP";"REG";"loypredm2";"lwr.IPm2";'
    '"upr.IPm2";"TYPPRED";"nbobs_com";"nbobs_mail";"R2_adj"\r\n'
    '"2920";"33063";"Bordeaux";"243300316";"33";"75";15,9931995588236;'
    '12,2720522168567;20,8426779489266;"commune";84641;96006;0,821385071888653\r\n'
    '"1";"17347";"Saint-Jean-d\'Angély";"200041689";"17";"75";7,57731321606059;'
    '5,99200114135791;9,58205351097027;"maille";646;878;0,648132867483885\r\n'
).encode("cp1252")


def rows(raw: bytes, stats: LoadStats | None = None) -> list[list]:
    text = loyers.decode(raw)
    return list(loyers.iter_loyers(io.StringIO(text, newline=""), stats or LoadStats()))


def test_decimal_comma_semicolons_and_cp1252():
    stats = LoadStats()
    bordeaux, angely = (dict(zip(loyers.LOYER_COLUMNS, r)) for r in rows(SAMPLE, stats))
    assert bordeaux["code_insee"] == "33063"
    assert bordeaux["loyer_m2"] == Decimal("15.9931995588236")
    assert bordeaux["r2_ajuste"] == Decimal("0.821385071888653")
    assert bordeaux["niveau_prediction"] == "commune"
    assert bordeaux["nb_obs_commune"] == 84641
    assert angely["nom_commune"] == "Saint-Jean-d'Angély"  # decoded from Windows-1252
    assert angely["niveau_prediction"] == "maille"
    assert stats.rows_loaded == 2 and stats.rows_rejected == 0


def test_utf8_files_are_accepted_too():
    assert rows(SAMPLE.decode("cp1252").encode("utf-8"))[1][1] == "Saint-Jean-d'Angély"


def test_gzip_bytes_as_served_by_data_gouv_are_unzipped():
    assert rows(gzip.compress(SAMPLE))[0][0] == "33063"


def test_a_row_without_a_rent_is_rejected():
    bad = SAMPLE.replace(b";15,9931995588236;", b";;")
    stats = LoadStats()
    assert [r[0] for r in rows(bad, stats)] == ["17347"]
    assert stats.rows_rejected == 1


def test_unparseable_optional_value_is_counted_for_the_format_gate():
    bad = SAMPLE.replace(b";0,821385071888653", b";n/a")
    stats = LoadStats()
    rows(bad, stats)
    assert stats.nulled["r2_ajuste"] == 1


def test_schema_drift_fails_fast():
    with pytest.raises(loyers.AirflowFailException, match="loypredm2"):
        rows(SAMPLE.replace(b"loypredm2", b"loyer"))


def test_pick_files_maps_the_four_segments():
    base = "https://static.data.gouv.fr/resources/x/2025"
    resources = [{"url": f"{base}/note-methodologique.pdf"}] + [
        {"url": f"{base}/{stem}.csv"} for stem in loyers.SEGMENTS
    ]
    files = loyers.pick_files(resources)
    assert set(files) == {
        "appartement",
        "appartement_1_2_pieces",
        "appartement_3_pieces_plus",
        "maison",
    }
    assert files["maison"].endswith("pred-mai-mef-dhup.csv")
