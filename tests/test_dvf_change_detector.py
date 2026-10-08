import dvf_change_detector as detector

INDEX_HTML = """<html><body><h1>Index of /geo-dvf/latest/csv/</h1>
<a href="../">../</a>
<a href="/geo-dvf/latest/csv/2021/">2021/</a>
<a href="/geo-dvf/latest/csv/2025/">2025/</a>
<a href="/geo-dvf/latest/csv/2024/">2024/</a>
<a href="/geo-dvf/latest/csv/1999/">1999/</a>
</body></html>"""


def test_list_available_years_sorted_and_filtered():
    assert detector.list_available_years(INDEX_HTML) == [2021, 2024, 2025]


def test_plan_reloads_new_changed_unchanged():
    remote = {
        2023: {"etag": '"a"', "size": 10},  # unchanged
        2024: {"etag": '"b2"', "size": 20},  # republished (new ETag)
        2025: {"etag": '"c"', "size": 30},  # never loaded
    }
    loaded = {
        2022: {"etag": '"z"', "size": 1},  # dropped from /latest: ignored
        2023: {"etag": '"a"', "size": 10},
        2024: {"etag": '"b1"', "size": 20},
    }
    plan = detector.plan_reloads(remote, loaded)
    assert [(c["year"], c["reason"]) for c in plan] == [
        (2024, "changed"),
        (2025, "new"),
    ]


def test_size_change_alone_triggers_reload():
    plan = detector.plan_reloads(
        {2024: {"etag": None, "size": 21}}, {2024: {"etag": None, "size": 20}}
    )
    assert [c["reason"] for c in plan] == ["changed"]


def test_trigger_run_ids_dedupe_per_file_version_and_day():
    change = {"year": 2024, "reason": "changed", "etag": '"b2"', "size": 20}

    def run_id(chg, day="20261009"):
        return detector.trigger_kwargs([chg], day)[0]["trigger_run_id"]

    first = detector.trigger_kwargs([change], "20261009")[0]
    assert first["conf"] == {"year": 2024}
    assert first["trigger_run_id"].startswith("dvf_change__2024__")
    assert first["trigger_run_id"].endswith("__20261009")
    # Same file, same day: identical run_id, so no duplicate load is queued.
    assert run_id(dict(change)) == first["trigger_run_id"]
    # Republished file: new run_id.
    assert run_id({**change, "etag": '"b3"'}) != first["trigger_run_id"]
    # Same file still pending the next day (previous load failed): retried.
    assert run_id(change, "20261010") != first["trigger_run_id"]
