from decimal import Decimal

import dvf_accessibilite_pipeline as acc


def test_suppressed_indicators_are_null_not_drift():
    assert acc.to_indicator("NS") is None
    assert acc.to_indicator("103.4") == Decimal("103.4")


def test_education_filter_accepts_both_department_formats():
    assert acc.edu_where("code_departement", ("33",)) == (
        'code_departement="033" OR code_departement="33"'
    )
