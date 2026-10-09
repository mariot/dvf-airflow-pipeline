"""DVF (Demandes de Valeurs Foncières) -> PostgreSQL.

Loads one yearly file of "DVF géolocalisées" (published by Etalab on
data.gouv.fr) into a LIST-partitioned PostgreSQL table, ``dvf.mutations``.

    download_dvf_file ─┐
                       ├─> load_dvf_into_postgres ─> refresh_semantic_layer
    sync_schema ───────┘

* ``download_dvf_file``: streams ``{DVF_BASE_URL}/{year}/full.csv.gz`` to the
  local ``data/`` volume in 1 MiB chunks. It writes to a ``.part`` file and
  then renames it atomically. It skips the download when the remote ETag and
  size match what is already on disk.
* ``sync_schema``: applies ``sql/init.sql`` (idempotent DDL) and makes sure
  the partition for the requested year exists.
* ``load_dvf_into_postgres``: reads the gzip file as a stream, cleans each
  record on the fly, and sends CHUNKS of rows to Postgres through an in-memory
  ``StringIO`` buffer and the native ``COPY ... FROM STDIN``.
* ``refresh_semantic_layer``: rebuilds the query-facing objects of
  ``sql/semantic.sql`` (``dvf.ventes``, ``dvf.biens``...) from ``dvf.mutations``.

History
    Loads are APPEND-BY-YEAR: a run only ever replaces its own year's
    partition. Etalab serves the last 5 years and drops the oldest one at each
    release, but a year that leaves ``/latest`` is never touched again, so the
    database keeps every year it has ever loaded. A republished year must not
    shrink by more than ``DVF_MAX_SHRINK_RATIO`` (default 20%) compared with
    its previous load, so a truncated upstream file can't erase history.

Format
    Etalab's files are comma-separated with decimal points (the raw DGFiP
    files use ``|`` and decimal commas). Columns are mapped by name, so a
    pipe-separated file fails the schema check, and an optional column whose
    values stop parsing (e.g. ``"346,5"``) fails the load once its NULLed ratio
    exceeds ``DVF_MAX_REJECT_RATIO`` instead of being silently NULLed.

Idempotency
    The year's partition is TRUNCATEd and reloaded in ONE transaction, along
    with its audit row in ``dvf.load_log``. A failure at any point rolls back
    to the previous state, and a rerun replaces the data instead of appending
    to it. The ``(annee, num_ligne_source)`` primary key also makes a
    duplicate load structurally impossible.

Memory
    RAM use is bounded by ``DVF_COPY_CHUNK_ROWS`` (default 50k rows, about
    15 MB of CSV text), whatever the file size (~4.7M rows / ~120 MB gzipped).

Configuration (Airflow Variables, read at RUN time, never at parse time)
    DVF_BASE_URL            https://files.data.gouv.fr/geo-dvf/latest/csv
    DVF_DATA_DIR            /opt/airflow/data/dvf
    DVF_SQL_DIR             /opt/airflow/sql
    DVF_POSTGRES_CONN_ID    dvf_postgres
    DVF_COPY_CHUNK_ROWS     50000
    DVF_MAX_REJECT_RATIO    0.001
    DVF_MAX_SHRINK_RATIO    0.2

Trigger for a given year:
    airflow dags trigger dvf_postgres_pipeline -c '{"year": 2024}'
"""

import csv
import gzip
import io
import json
import logging
import math
import re
import time
from collections import Counter
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, NamedTuple

import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Param, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException

from dvf_common import HTTP_TIMEOUT, LOADER_DAG_ID, MIN_YEAR, DvfConfig

log = logging.getLogger(__name__)

DAG_ID = LOADER_DAG_ID
DEFAULT_YEAR = datetime.now(tz=UTC).year - 1

# Serializes DDL across concurrent DAG runs (CREATE ... IF NOT EXISTS is not
# race-free in PostgreSQL). Any constant bigint works.
SCHEMA_ADVISORY_LOCK_ID = 7_301_947_001

IO_CHUNK_BYTES = 1024 * 1024  # download chunks and COPY writes


def parse_http_date(value: str | None) -> datetime | None:
    """'Mon, 18 May 2026 12:56:30 GMT' -> aware datetime (None if absent/invalid)."""
    if not value:
        return None
    try:
        return parsedate_to_datetime(value)
    except TypeError, ValueError:
        return None


def partition_name(year: int) -> str:
    """Fully-qualified partition name.

    ``year`` is always a validated int (see ``_year_from_context``), so
    interpolating it into SQL is injection-safe.
    """
    return f"dvf.mutations_{int(year)}"


def _year_from_context() -> int:
    year = int(get_current_context()["params"]["year"])
    if not MIN_YEAR <= year <= datetime.now(tz=UTC).year:
        raise AirflowFailException(f"year={year} is outside [{MIN_YEAR}, today]")
    return year


# =============================================================================
# Cleaning: typed, null-safe converters
# -----------------------------------------------------------------------------
# Every converter maps '' to None (written as an unquoted empty field, which
# COPY ... (FORMAT csv) reads as NULL). It raises ValueError when a value
# cannot be parsed. The caller decides what that means: the row is rejected if
# the column is required, otherwise the value is set to NULL and counted.
# =============================================================================
_CODE_COMMUNE_RE = re.compile(r"^(?:\d{5}|2[AB]\d{3})$")
_CODE_DEPARTEMENT_RE = re.compile(r"^(?:\d{2,3}|2[AB])$")


def to_text(value: str) -> str | None:
    return value.strip() or None


def to_int(value: str) -> int | None:
    """'000001' -> 1, '42' -> 42, '3.0' -> 3, '45.6' -> 46 (float casting)."""
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        number = float(value)  # raises ValueError on garbage
        if not math.isfinite(number):
            raise ValueError(f"non-finite integer value {value!r}") from None
        return round(number)


def to_decimal(value: str) -> Decimal | None:
    """Exact decimal for money and surfaces: no binary-float rounding error."""
    if not value:
        return None
    try:
        number = Decimal(value)
    except InvalidOperation:
        raise ValueError(f"invalid decimal {value!r}") from None
    if not number.is_finite():
        raise ValueError(f"non-finite decimal {value!r}")
    return number


def to_float(value: str) -> float | None:
    if not value:
        return None
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(f"non-finite float {value!r}")
    return number


def to_latitude(value: str) -> float | None:
    number = to_float(value)
    if number is not None and not -90.0 <= number <= 90.0:
        raise ValueError(f"latitude out of range {value!r}")
    return number


def to_longitude(value: str) -> float | None:
    number = to_float(value)
    if number is not None and not -180.0 <= number <= 180.0:
        raise ValueError(f"longitude out of range {value!r}")
    return number


def to_iso_date(value: str) -> str | None:
    if not value:
        return None
    date.fromisoformat(value)  # validation only; Postgres parses ISO dates natively
    return value


def _matching(pattern: re.Pattern[str]) -> Callable[[str], str | None]:
    def convert(value: str) -> str | None:
        value = value.strip()
        if not value:
            return None
        if not pattern.match(value):
            raise ValueError(f"{value!r} does not match {pattern.pattern}")
        return value

    return convert


to_code_commune = _matching(_CODE_COMMUNE_RE)
to_code_departement = _matching(_CODE_DEPARTEMENT_RE)


class Column(NamedTuple):
    name: str
    convert: Callable[[str], Any]
    required: bool = False  # True: a NULL or invalid value rejects the whole row


# Source columns, in the order of sql/init.sql (from annee/num_ligne_source on).
# Source fields are mapped BY NAME, so a column reorder upstream is harmless,
# and a missing column fails fast.
COLUMNS: tuple[Column, ...] = (
    Column("id_mutation", to_text, required=True),
    Column("date_mutation", to_iso_date, required=True),
    Column("numero_disposition", to_int),
    Column("nature_mutation", to_text),
    Column("valeur_fonciere", to_decimal),
    Column("adresse_numero", to_int),
    Column("adresse_suffixe", to_text),
    Column("adresse_nom_voie", to_text),
    Column("adresse_code_voie", to_text),
    Column("code_postal", to_text),
    Column("code_commune", to_code_commune, required=True),
    Column("nom_commune", to_text),
    Column("code_departement", to_code_departement, required=True),
    Column("ancien_code_commune", to_text),
    Column("ancien_nom_commune", to_text),
    Column("id_parcelle", to_text),
    Column("ancien_id_parcelle", to_text),
    Column("numero_volume", to_text),
    Column("lot1_numero", to_text),
    Column("lot1_surface_carrez", to_decimal),
    Column("lot2_numero", to_text),
    Column("lot2_surface_carrez", to_decimal),
    Column("lot3_numero", to_text),
    Column("lot3_surface_carrez", to_decimal),
    Column("lot4_numero", to_text),
    Column("lot4_surface_carrez", to_decimal),
    Column("lot5_numero", to_text),
    Column("lot5_surface_carrez", to_decimal),
    Column("nombre_lots", to_int),
    Column("code_type_local", to_int),
    Column("type_local", to_text),
    Column("surface_reelle_bati", to_int),
    Column("nombre_pieces_principales", to_int),
    Column("code_nature_culture", to_text),
    Column("nature_culture", to_text),
    Column("code_nature_culture_speciale", to_text),
    Column("nature_culture_speciale", to_text),
    Column("surface_terrain", to_int),
    Column("longitude", to_longitude),
    Column("latitude", to_latitude),
)

TARGET_COLUMNS: tuple[str, ...] = (
    "annee",
    "num_ligne_source",
    *(c.name for c in COLUMNS),
)


class RejectedRow(ValueError):
    """Raised when a record can't be loaded (bad shape or invalid required field)."""


@dataclass(slots=True)
class LoadStats:
    rows_read: int = 0
    rows_loaded: int = 0
    rows_rejected: int = 0
    chunks: int = 0
    nulled: Counter[str] = field(default_factory=Counter)

    @property
    def reject_ratio(self) -> float:
        return self.rows_rejected / self.rows_read if self.rows_read else 0.0

    def nulled_ratios(self) -> dict[str, float]:
        """Share of rows whose value was unparseable and set to NULL, per column."""
        if not self.rows_read:
            return {}
        return {name: n / self.rows_read for name, n in self.nulled.items()}


def check_format(stats: LoadStats, max_ratio: float) -> None:
    """Fail when a column's values stopped parsing (format drift, e.g. decimal commas)."""
    drifted = {
        name: ratio
        for name, ratio in stats.nulled_ratios().items()
        if ratio > max_ratio
    }
    if drifted:
        detail = ", ".join(
            f"{name}={ratio:.2%}" for name, ratio in sorted(drifted.items())
        )
        raise AirflowFailException(
            f"Format drift: unparseable values above {max_ratio:.4%} in {detail}"
        )


def check_history(
    rows_loaded: int, previous_rows: int | None, max_shrink: float
) -> None:
    """Refuse to replace a year with a much smaller version of itself."""
    if previous_rows and rows_loaded < previous_rows * (1 - max_shrink):
        raise AirflowFailException(
            f"Year would shrink from {previous_rows:,} to {rows_loaded:,} rows "
            f"(more than DVF_MAX_SHRINK_RATIO={max_shrink:.0%}); keeping the previous load"
        )


def build_plan(header: list[str]) -> list[tuple[int, Column]]:
    """Map each target column to its index in the source header."""
    positions = {name: i for i, name in enumerate(header)}
    missing = [c.name for c in COLUMNS if c.name not in positions]
    if missing:
        raise AirflowFailException(f"Source schema drift: missing columns {missing}")
    extra = sorted(set(header) - {c.name for c in COLUMNS})
    if extra:
        log.warning("Ignoring unexpected source columns: %s", extra)
    return [(positions[c.name], c) for c in COLUMNS]


def clean_record(
    record: list[str],
    plan: list[tuple[int, Column]],
    width: int,
    nulled: Counter[str],
) -> list[Any]:
    """Return one cleaned, typed row (without annee/num_ligne_source).

    Raises RejectedRow when the record can't be loaded.
    """
    if len(record) != width:
        raise RejectedRow(f"expected {width} fields, got {len(record)}")
    row: list[Any] = []
    for index, column in plan:
        try:
            value = column.convert(record[index])
        except ValueError as exc:
            if column.required:
                raise RejectedRow(f"{column.name}: {exc}") from None
            nulled[column.name] += 1
            value = None
        if value is None and column.required:
            raise RejectedRow(f"{column.name} is required")
        row.append(value)
    return row


def iter_clean_rows(
    lines: Iterable[str], year: int, stats: LoadStats
) -> Iterator[list[Any]]:
    """Yield ``[annee, num_ligne_source, *cleaned columns]`` for every valid record."""
    reader = csv.reader(lines)
    try:
        header = next(reader)
    except StopIteration:
        raise AirflowFailException("Source file is empty") from None
    plan = build_plan(header)
    width = len(header)

    for line_number, record in enumerate(reader, start=1):
        stats.rows_read += 1
        try:
            row = clean_record(record, plan, width, stats.nulled)
        except RejectedRow as exc:
            stats.rows_rejected += 1
            if stats.rows_rejected <= 10:  # sample, don't flood the logs
                log.warning("Rejected line %d: %s | %r", line_number, exc, record[:4])
            continue
        yield [year, line_number, *row]


# =============================================================================
# Postgres COPY: driver-agnostic
# -----------------------------------------------------------------------------
# With SQLAlchemy 2 + psycopg 3 installed, which is the case under the Airflow
# 3.x constraints, PostgresHook.get_conn() returns a *psycopg 3* connection.
# Its cursors have no ``copy_expert`` (that's psycopg2). Both APIs are
# supported here, so the DAG works whichever driver the hook hands out.
# =============================================================================
def copy_buffer(cursor: Any, copy_sql: str, buffer: io.StringIO) -> None:
    buffer.seek(0)
    if hasattr(cursor, "copy_expert"):  # psycopg2
        cursor.copy_expert(copy_sql, buffer)
    else:  # psycopg 3
        with cursor.copy(copy_sql) as copy:
            while data := buffer.read(IO_CHUNK_BYTES):
                copy.write(data)


def stream_rows_into_postgres(
    cursor: Any,
    rows: Iterable[list[Any]],
    table: str,
    chunk_rows: int,
    stats: LoadStats,
) -> None:
    """Serialize rows as CSV into a bounded StringIO and COPY it chunk by chunk.

    Only ONE chunk is held in memory at a time. The caller owns the
    transaction: nothing is committed here.
    """
    copy_sql = (
        f"COPY {table} ({', '.join(TARGET_COLUMNS)}) "
        "FROM STDIN WITH (FORMAT csv, HEADER false)"
    )
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    pending = 0

    def flush() -> None:
        nonlocal buffer, writer, pending
        if not pending:
            return
        copy_buffer(cursor, copy_sql, buffer)
        stats.rows_loaded += pending
        stats.chunks += 1
        if stats.chunks % 10 == 0:
            log.info("COPY progress: %s rows loaded", f"{stats.rows_loaded:,}")
        # A fresh buffer releases the previous chunk's memory immediately.
        buffer = io.StringIO()
        writer = csv.writer(buffer, lineterminator="\n")
        pending = 0

    for row in rows:
        writer.writerow(row)
        pending += 1
        if pending >= chunk_rows:
            flush()
    flush()


# =============================================================================
# Download
# =============================================================================
def download_file(url: str, destination: Path) -> dict[str, Any]:
    """Stream ``url`` to ``destination`` without loading it into RAM.

    Idempotent: a sidecar ``.meta.json`` stores the remote ETag and size, and
    the download is skipped when they still match. Writes go to a ``.part``
    file that is renamed atomically, so a crash never leaves a truncated file
    under the final name.
    """
    destination.parent.mkdir(parents=True, exist_ok=True)
    meta_path = destination.with_name(destination.name + ".meta.json")
    part_path = destination.with_name(destination.name + ".part")

    with requests.Session() as session:
        head = session.head(url, allow_redirects=True, timeout=HTTP_TIMEOUT)
        if head.status_code == 404:
            # Not retryable: data.gouv.fr only serves the last 5 years under /latest.
            raise AirflowFailException(f"No DVF file at {url}")
        head.raise_for_status()
        remote = {
            "url": url,
            "etag": head.headers.get("ETag"),
            "size": int(head.headers.get("Content-Length", -1)),
            "last_modified": head.headers.get("Last-Modified"),
        }

        if destination.exists() and meta_path.exists():
            local = json.loads(meta_path.read_text())
            if (
                remote["etag"]
                and local.get("etag") == remote["etag"]
                and destination.stat().st_size == remote["size"]
            ):
                log.info("Up to date, skipping download: %s", destination)
                return {**remote, "path": str(destination), "downloaded": False}

        log.info("Downloading %s -> %s", url, destination)
        written = 0
        started = time.monotonic()
        with session.get(url, stream=True, timeout=HTTP_TIMEOUT) as response:
            response.raise_for_status()
            # Re-read metadata from the GET itself, in case the file changed after the HEAD.
            remote["etag"] = response.headers.get("ETag", remote["etag"])
            remote["size"] = int(response.headers.get("Content-Length", remote["size"]))
            remote["last_modified"] = response.headers.get(
                "Last-Modified", remote["last_modified"]
            )
            with part_path.open("wb") as fh:
                # decode_content=False: store the .gz bytes exactly as served,
                # even if a proxy adds a Content-Encoding header.
                for chunk in response.raw.stream(IO_CHUNK_BYTES, decode_content=False):
                    fh.write(chunk)
                    written += len(chunk)

    if remote["size"] >= 0 and written != remote["size"]:
        part_path.unlink(missing_ok=True)
        raise OSError(f"Truncated download: {written} / {remote['size']} bytes")

    part_path.replace(destination)  # atomic on the same filesystem
    meta_path.write_text(json.dumps(remote))
    log.info(
        "Downloaded %.1f MiB in %.1fs", written / 2**20, time.monotonic() - started
    )
    return {**remote, "path": str(destination), "downloaded": True}


# =============================================================================
# DAG
# =============================================================================
default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
}


@dag(
    dag_id=DAG_ID,
    description="Load DVF géolocalisées (data.gouv.fr) for one year into PostgreSQL",
    schedule=None,  # on demand; Etalab republishes DVF twice a year (April / October)
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=3,  # different years load in parallel into different partitions
    dagrun_timeout=timedelta(hours=2),
    default_args=default_args,
    params={
        "year": Param(
            DEFAULT_YEAR,
            type="integer",
            minimum=MIN_YEAR,
            description="DVF year to (re)load. /latest serves the last 5 years.",
        ),
    },
    tags=["data_processing", "dvf", "postgres"],
    doc_md=__doc__,
)
def dvf_postgres_pipeline() -> None:
    @task(execution_timeout=timedelta(minutes=30))
    def download_dvf_file() -> dict[str, Any]:
        """Task 1: stream the yearly .csv.gz to disk."""
        cfg = DvfConfig.from_airflow()
        year = _year_from_context()
        return download_file(cfg.source_url(year), cfg.local_path(year))

    @task(execution_timeout=timedelta(minutes=10))
    def sync_schema() -> str:
        """Task 2: apply init.sql (idempotent) and make sure the year's partition exists."""
        cfg = DvfConfig.from_airflow()
        year = _year_from_context()
        ddl = (cfg.sql_dir / "init.sql").read_text(encoding="utf-8")
        partition = partition_name(year)

        hook = PostgresHook(postgres_conn_id=cfg.conn_id)
        conn = hook.get_conn()
        try:
            with conn.cursor() as cur:
                # Transaction-scoped: released automatically on commit/rollback.
                cur.execute(
                    "SELECT pg_advisory_xact_lock(%s)", (SCHEMA_ADVISORY_LOCK_ID,)
                )
                cur.execute(ddl)
                cur.execute(
                    f"CREATE TABLE IF NOT EXISTS {partition} "
                    f"PARTITION OF dvf.mutations FOR VALUES IN ({year})"
                )
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()
        log.info("Schema in sync, partition %s ready", partition)
        return partition

    @task(execution_timeout=timedelta(hours=1))
    def load_dvf_into_postgres(
        source: dict[str, Any], partition: str
    ) -> dict[str, Any]:
        """Task 3: TRUNCATE the partition and COPY the cleaned rows, atomically."""
        cfg = DvfConfig.from_airflow()
        year = _year_from_context()
        context = get_current_context()
        stats = LoadStats()
        started = time.monotonic()

        hook = PostgresHook(postgres_conn_id=cfg.conn_id)
        conn = hook.get_conn()
        try:
            with conn.cursor() as cur:
                # Long enough to queue behind a concurrent load of the same year
                # (~2-3 min), short enough to fail on a genuinely stuck lock.
                cur.execute("SET LOCAL lock_timeout = '5min'")
                # Idempotency: the reload REPLACES the year. TRUNCATE is
                # transactional: on failure the old rows come back untouched.
                # It takes an ACCESS EXCLUSIVE lock, so queries on THIS
                # partition wait until commit. Other years stay readable.
                cur.execute(f"TRUNCATE TABLE {partition}")

                with gzip.open(
                    source["path"], "rt", encoding="utf-8", newline=""
                ) as fh:
                    rows = iter_clean_rows(fh, year, stats)
                    stream_rows_into_postgres(
                        cur, rows, partition, cfg.chunk_rows, stats
                    )

                # ---- Quality gates (any failure rolls everything back) ----------
                if stats.rows_loaded == 0:
                    raise AirflowFailException(
                        "No rows loaded; refusing to commit an empty year"
                    )
                if stats.reject_ratio > cfg.max_reject_ratio:
                    raise AirflowFailException(
                        f"Reject ratio {stats.reject_ratio:.4%} exceeds "
                        f"DVF_MAX_REJECT_RATIO={cfg.max_reject_ratio:.4%} "
                        f"({stats.rows_rejected:,} / {stats.rows_read:,} rows)"
                    )
                check_format(stats, cfg.max_reject_ratio)
                cur.execute(f"SELECT count(*) FROM {partition}")
                (db_count,) = cur.fetchone()
                if db_count != stats.rows_loaded:
                    raise AirflowFailException(
                        f"Row count mismatch: streamed {stats.rows_loaded:,}, table has {db_count:,}"
                    )
                cur.execute(
                    "SELECT rows_loaded FROM dvf.v_load_status WHERE annee = %s",
                    (year,),
                )
                previous = cur.fetchone()
                check_history(
                    stats.rows_loaded,
                    previous[0] if previous else None,
                    cfg.max_shrink_ratio,
                )

                cur.execute(
                    """
                    INSERT INTO dvf.load_log (
                        annee, source_url, source_etag, source_size_bytes,
                        source_last_modified,
                        rows_read, rows_loaded, rows_rejected, values_nulled,
                        dag_run_id, duration_seconds
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        year,
                        source["url"],
                        source.get("etag"),
                        source.get("size"),
                        parse_http_date(source.get("last_modified")),
                        stats.rows_read,
                        stats.rows_loaded,
                        stats.rows_rejected,
                        sum(stats.nulled.values()),
                        context["run_id"],
                        round(time.monotonic() - started, 1),
                    ),
                )
                # Fresh planner statistics on the reloaded partition.
                cur.execute(f"ANALYZE {partition}")
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()

        summary = {
            "year": year,
            "partition": partition,
            "rows_read": stats.rows_read,
            "rows_loaded": stats.rows_loaded,
            "rows_rejected": stats.rows_rejected,
            "values_nulled": dict(stats.nulled),
            "chunks": stats.chunks,
            "duration_seconds": round(time.monotonic() - started, 1),
        }
        log.info("Load committed: %s", summary)
        return summary

    @task(execution_timeout=timedelta(minutes=30))
    def refresh_semantic_layer(summary: dict[str, Any]) -> None:
        """Task 4: rebuild sql/semantic.sql (derived views + reference tables)."""
        cfg = DvfConfig.from_airflow()
        ddl = (cfg.sql_dir / "semantic.sql").read_text(encoding="utf-8")
        hook = PostgresHook(postgres_conn_id=cfg.conn_id)
        conn = hook.get_conn()
        try:
            with conn.cursor() as cur:
                # Same lock as sync_schema: parallel year loads rebuild one at a time.
                cur.execute(
                    "SELECT pg_advisory_xact_lock(%s)", (SCHEMA_ADVISORY_LOCK_ID,)
                )
                cur.execute(ddl)
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()
        log.info("Semantic layer rebuilt after loading %s", summary["year"])

    refresh_semantic_layer(
        load_dvf_into_postgres(source=download_dvf_file(), partition=sync_schema())
    )


dvf_postgres_pipeline()
