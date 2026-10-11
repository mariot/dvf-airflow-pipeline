from decimal import Decimal

import dvf_logement_pipeline as lg
import pytest
from dvf_io import LoadStats

# ----------------------------------------------------------------------- RPLS
RPLS_2025 = (
    '"DEP_CODE";"EPCI_CODE";"DEPCOM";"CODEPOSTAL";"LIBCOM";"QPV_LIBELLE";"TYPECONST_LIBELLE";'
    '"NBPIECE";"SURFHAB";"CONSTRUCT";"LOCAT";"FINAN_LIBELLE";"DROIT_LIBELLE";"DPEENERGIE";'
    '"DPESERRE";"EPSG";"X";"Y";"PLG_IRIS2024_CODE"\n'
    '"33";"200070092";"33001";"33230";"Abzac";"Non";"individuel";4;78;"1950";"1995";'
    '"PLA ordinaire";"pleine propriété";"D";"E";"2154";453825.58;6439918.56;"33001XXXX"\n'
)


def rpls(text: str) -> list[dict]:
    return [dict(zip(lg.RPLS_SPEC, row)) for row in lg.iter_rpls(text, LoadStats())]


def test_rpls_2025_layout():
    (row,) = rpls(RPLS_2025)
    assert row["code_commune"] == "33001" and row["nom_commune"] == "Abzac"
    assert row["nb_pieces"] == 4 and row["surface_habitable"] == Decimal(78)
    assert row["annee_construction"] == 1950
    assert (row["x"], row["y"]) == (Decimal("453825.58"), Decimal("6439918.56"))
    assert row["code_iris"] == "33001XXXX"


def test_rpls_2022_layout_uses_the_older_column_names():
    old = (
        RPLS_2025.replace('"DEPCOM"', '"DEPCOM_CODE"')
        .replace('"LIBCOM"', '"DEPCOM_LIBELLE"')
        .replace("PLG_IRIS2024_CODE", "PLG_IRIS2022_CODE")
    )
    assert rpls(old) == rpls(RPLS_2025)


def test_rpls_without_a_commune_column_fails_fast():
    with pytest.raises(lg.AirflowFailException, match="DEPCOM"):
        rpls(RPLS_2025.replace('"DEPCOM"', '"COMMUNE"'))


# ---------------------------------------------------------------------- LOVAC
LOVAC_COMMUNES = (
    '"CODGEO_26";"LIBGEO_26";"pp_vacant_26";"pp_vacant_plus_2ans_26";"ff_pp_total_25";'
    '"pp_vacant_25";"pp_vacant_plus_2ans_25";"EPCI_26"\r\n'
    '"33063";"Bordeaux";"16796";"3779";"150727";"11774";"4746";"243300316"\r\n'
    '"01002";"L\'Abergement-de-Varey";"16";"s";"180";"s";"s";"240100883"\r\n'
)
LOVAC_REGIONS = (
    '" REG ";" LIB_REG ";" ff_pp_total_22 ";" pp_vacant_22 ";" pp_vacant_longue_duree_22 "\r\n'
    '" 75 ";" Nouvelle-Aquitaine ";" 3 456 789    ";" 301 234    ";" 98 765    "\r\n'
)
LOVAC_FRANCE = (
    '"FR";"Millésime";"ff_pp_total";"pp_vacant";"pp_vacant_plus_2ans"\r\n'
    '"France";2026;;" 3 160 235    ";" 1 179 845    "\r\n'
    ';2025;" 33 194 351    ";" 2 380 076    ";" 1 348 470    "\r\n'
)


def test_lovac_communes_are_unpivoted_and_secrets_stay_null():
    rows = {
        (r[0], r[2]): r
        for r in lg.iter_lovac_wide(
            LOVAC_COMMUNES, "commune", "CODGEO_", "LIBGEO_", LoadStats()
        )
    }
    assert rows[(2026, "33063")] == [
        2026,
        "commune",
        "33063",
        "Bordeaux",
        None,
        16796,
        False,
        3779,
        False,
    ]
    assert rows[(2025, "33063")][4:] == [150727, 11774, False, 4746, False]
    # "s" = statistical secret: NULL plus a flag, never 0
    assert rows[(2025, "01002")][4:] == [180, None, True, None, True]


def test_lovac_regions_thousands_separators_and_renamed_column():
    (row,) = lg.iter_lovac_wide(LOVAC_REGIONS, "region", "REG", "LIB_REG", LoadStats())
    assert row == [
        2022,
        "region",
        "75",
        "Nouvelle-Aquitaine",
        3456789,
        301234,
        False,
        98765,
        False,
    ]


def test_lovac_france_long_file():
    rows = list(lg.iter_lovac_france(LOVAC_FRANCE, LoadStats()))
    assert rows[0] == [
        2026,
        "france",
        "FR",
        "France",
        None,
        3160235,
        False,
        1179845,
        False,
    ]
    assert rows[1][4] == 33194351


def test_lovac_files_requires_the_four_levels():
    with pytest.raises(lg.AirflowFailException, match="Lovac_opendata_France.csv"):
        lg.lovac_files([{"title": "Lovac_opendata_Communes.csv", "url": "u"}])


# ---------------------------------------------------------------- long series
def workbook() -> bytes:
    xlwt = pytest.importorskip("xlwt")
    import io

    book = xlwt.Workbook()
    sheet = book.add_sheet("Series")
    sheet.write(0, 0, "Français")
    sheet.write(3, 0, "Série")
    for i, year in enumerate(range(1800, 2021)):
        sheet.write(3, 3 + i, year)
    sheet.write(4, 0, "A. France")
    sheet.write(5, 0, "A.d. Séries immobilières")
    sheet.write(6, 0, "Indice du prix des logements, France (€, base 2000)")
    sheet.write(6, 1, "Home price index, France (€, basis 2000)")
    sheet.write(6, 2, "https://www.insee.fr")
    sheet.write(6, 3 + 200, 55.5)  # 2000
    sheet.write(6, 3 + 220, 151.2)  # 2020
    buffer = io.BytesIO()
    book.save(buffer)
    return buffer.getvalue()


def test_series_longues_unpivot_with_country_and_category():
    rows = list(lg.iter_series_longues(workbook(), LoadStats()))
    assert rows == [
        [
            "France",
            "Séries immobilières",
            "Indice du prix des logements, France (€, base 2000)",
            "Home price index, France (€, basis 2000)",
            "https://www.insee.fr",
            2000,
            55.5,
        ],
        [
            "France",
            "Séries immobilières",
            "Indice du prix des logements, France (€, base 2000)",
            "Home price index, France (€, basis 2000)",
            "https://www.insee.fr",
            2020,
            151.2,
        ],
    ]


# ------------------------------------------------------------------------ CDC
CDC = (
    "\ufeffcode_region;region;annee_signature;construction_surface_mediane_des_operations_m2_su;"
    "construction_prix_de_revient_median_au_m2_des_operations;"
    "construction_prix_de_revient_median_des_operations_au_logement;"
    "rehabilitation_surface_mediane_des_operations_m2_su;"
    "rehabilitation_prix_de_revient_median_au_m2_des_operations;"
    "rehabilitation_prix_de_revient_median_des_operations_au_logement;geom;centroid\n"
    '75;NOUVELLE-AQUITAINE;2025;66;2605;170663;;;;"{""coordinates"": [[[0, 1]]]}";"x"\n'
)


def test_cdc_keeps_empty_rehabilitation_values_null():
    (row,) = lg.iter_cdc(lg.decode(CDC.encode("utf-8")), LoadStats())
    values = dict(zip(lg.CDC_SPEC, row))
    assert (values["code_region"], values["annee"]) == ("75", 2025)
    assert values["construction_cout_m2"] == Decimal(2605)
    assert values["rehabilitation_cout_m2"] is None


def test_to_count_strips_thousands_separators():
    assert lg.to_count(" 3 160 235    ") == 3160235
    assert lg.to_count("1\u202f179\u00a0845") == 1179845


def test_files_served_gzip_compressed_are_read_as_text(tmp_path):
    import gzip

    path = tmp_path / "lovac-opendata-communes26.csv"  # .csv name, gzip content
    path.write_bytes(gzip.compress(LOVAC_COMMUNES.encode("cp1252")))
    text = lg.decode(lg.read_downloaded(path))
    assert text.startswith('"CODGEO_26"')
