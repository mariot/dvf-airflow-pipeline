"""Detect new or changed DVF files on data.gouv.fr and trigger targeted reloads.

    detect_changes ──> trigger_reload (mapped: one dvf_postgres_pipeline run per year)

How the detection works
    1. List the years currently published under ``DVF_BASE_URL`` (the HTML
       index of ``.../geo-dvf/latest/csv/``).
    2. Send a ``HEAD`` to each ``{year}/full.csv.gz`` and record its ETag and size.
       Nothing gets downloaded.
    3. Compare with the latest COMMITTED load of that year in ``dvf.load_log``,
       which stores the ETag and size of the exact file that was loaded.
         * year never loaded         -> "new"
         * ETag or size differs      -> "changed" (Etalab republished it)
         * otherwise                 -> unchanged, nothing to do
    4. Trigger ``dvf_postgres_pipeline`` with ``{"year": ...}`` for each
       new/changed year only.

The database is the source of truth: a reload that failed or rolled back
leaves the old ETag in ``load_log``, so the year is detected again on the next run.

Each triggered run_id is deterministic per (year, file version, detection day),
e.g. ``dvf_change__2024__3f1c9a0b2e7d__20261009``:
  * Running the detector several times on the same day never queues a
    duplicate load (``skip_when_already_exists``).
  * A load that failed is retried by the next daily run, because the year
    still shows up as changed and the day in the run_id is different. A
    run_id built from the file version alone would block that retry forever.

Years that disappear from /latest (Etalab keeps 5 years) are only logged.
Their data is kept in Postgres.

Etalab republishes DVF about twice a year (April / October). One HEAD per year
per day is negligible, so the detector runs daily.
"""

import hashlib
import logging
import re
from datetime import UTC, datetime, timedelta
from typing import Any

import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.trigger_dagrun import (
    TriggerDagRunOperator,
)
from airflow.sdk import dag, get_current_context, task

from dvf_common import (
    DETECTOR_DAG_ID,
    HTTP_TIMEOUT,
    LOADER_DAG_ID,
    MIN_YEAR,
    DvfConfig,
)

log = logging.getLogger(__name__)

DAG_ID = DETECTOR_DAG_ID

# Matches the year sub-folders in the nginx index: href="/geo-dvf/latest/csv/2024/"
_YEAR_DIR_RE = re.compile(r'href="[^"]*?/(\d{4})/"')


def list_available_years(index_html: str) -> list[int]:
    """Years published in the directory index, oldest first."""
    years = {int(y) for y in _YEAR_DIR_RE.findall(index_html)}
    return sorted(y for y in years if y >= MIN_YEAR)


def fetch_remote_state(cfg: DvfConfig, years: list[int]) -> dict[int, dict[str, Any]]:
    """HEAD each yearly file and return ``{year: {"etag": ..., "size": ...}}``."""
    remote: dict[int, dict[str, Any]] = {}
    with requests.Session() as session:
        for year in years:
            url = cfg.source_url(year)
            response = session.head(url, allow_redirects=True, timeout=HTTP_TIMEOUT)
            if response.status_code == 404:
                log.warning("Year %s listed but %s is missing; skipping", year, url)
                continue
            response.raise_for_status()
            remote[year] = {
                "etag": response.headers.get("ETag"),
                "size": int(response.headers.get("Content-Length", -1)),
            }
    return remote


def fetch_loaded_state(hook: PostgresHook) -> dict[int, dict[str, Any]]:
    """Latest committed load per year, from dvf.load_log (empty if no schema yet)."""
    if hook.get_first("SELECT to_regclass('dvf.load_log')")[0] is None:
        return {}
    rows = hook.get_records(
        """
        SELECT DISTINCT ON (annee) annee, source_etag, source_size_bytes
        FROM dvf.load_log
        ORDER BY annee, loaded_at DESC
        """
    )
    return {int(y): {"etag": etag, "size": size} for y, etag, size in rows}


def plan_reloads(
    remote: dict[int, dict[str, Any]], loaded: dict[int, dict[str, Any]]
) -> list[dict[str, Any]]:
    """Pure decision logic: which years to (re)load, and why."""
    changes = []
    for year, current in sorted(remote.items()):
        previous = loaded.get(year)
        if previous is None:
            reason = "new"
        elif current["etag"] != previous["etag"] or current["size"] != previous["size"]:
            reason = "changed"
        else:
            continue
        changes.append({"year": year, "reason": reason, **current})
    return changes


def trigger_kwargs(
    changes: list[dict[str, Any]], detected_on: str
) -> list[dict[str, Any]]:
    """One TriggerDagRunOperator kwargs dict per year, with a deterministic run_id.

    ``detected_on`` (YYYYMMDD) scopes deduplication to one day. See the module docstring.
    """
    kwargs = []
    for change in changes:
        fingerprint = f"{change['etag']}|{change['size']}".encode()
        digest = hashlib.sha256(fingerprint).hexdigest()[:12]
        kwargs.append(
            {
                "conf": {"year": change["year"]},
                "trigger_run_id": (
                    f"dvf_change__{change['year']}__{digest}__{detected_on}"
                ),
                "note": f"{change['reason']}: ETag {change['etag']}",
            }
        )
    return kwargs


@dag(
    dag_id=DAG_ID,
    description="Detect new/changed DVF files on data.gouv.fr and trigger reloads",
    schedule="0 6 * * *",  # daily 06:00 UTC
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["data_processing", "dvf", "monitoring"],
    doc_md=__doc__,
)
def dvf_change_detector() -> None:
    @task(execution_timeout=timedelta(minutes=10))
    def detect_changes() -> list[dict[str, Any]]:
        cfg = DvfConfig.from_airflow()

        index = requests.get(f"{cfg.base_url}/", timeout=HTTP_TIMEOUT)
        index.raise_for_status()
        years = list_available_years(index.text)
        if not years:
            raise ValueError(f"No year folders found at {cfg.base_url}/")
        log.info("Published years: %s", years)

        remote = fetch_remote_state(cfg, years)
        loaded = fetch_loaded_state(PostgresHook(postgres_conn_id=cfg.conn_id))

        if dropped := sorted(set(loaded) - set(remote)):
            log.info("No longer published (data kept in Postgres): %s", dropped)

        changes = plan_reloads(remote, loaded)
        for change in changes:
            log.info("Year %(year)s is %(reason)s (ETag %(etag)s)", change)
        if not changes:
            log.info("All published years are up to date")
        # The detector run's own date, so a retry of this run reuses the same
        # run_ids instead of the wall-clock date.
        detected_on = get_current_context()["dag_run"].run_after.strftime("%Y%m%d")
        return trigger_kwargs(changes, detected_on)

    # Zero changes -> zero mapped instances -> the task is simply skipped.
    TriggerDagRunOperator.partial(
        task_id="trigger_reload",
        trigger_dag_id=LOADER_DAG_ID,
        skip_when_already_exists=True,
        wait_for_completion=False,
    ).expand_kwargs(detect_changes())


dvf_change_detector()
