"""I/O helpers shared by the DVF DAGs. This module defines NO DAG.

DAG files must not import each other (the dag-processor would register the
imported DAG twice), so what several DAGs need lives here: typed converters,
load statistics and the format gate, the driver-agnostic COPY, and the
idempotent streaming download. Listed in .airflowignore.
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
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any

import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk.exceptions import AirflowFailException

from dvf_common import HTTP_TIMEOUT, DvfConfig

log = logging.getLogger(__name__)

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


def copy_buffer(cursor: Any, copy_sql: str, buffer: io.StringIO) -> None:
    buffer.seek(0)
    if hasattr(cursor, "copy_expert"):  # psycopg2
        cursor.copy_expert(copy_sql, buffer)
    else:  # psycopg 3
        with cursor.copy(copy_sql) as copy:
            while data := buffer.read(IO_CHUNK_BYTES):
                copy.write(data)


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


def apply_sql_file(cfg: DvfConfig, filename: str) -> None:
    """Run one of the idempotent SQL files of DVF_SQL_DIR under the schema lock."""
    ddl = (cfg.sql_dir / filename).read_text(encoding="utf-8")
    hook = PostgresHook(postgres_conn_id=cfg.conn_id)
    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(%s)", (SCHEMA_ADVISORY_LOCK_ID,))
            cur.execute(ddl)
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()


SEMANTIC_FILES = ("semantic.sql", "semantic_cadre_de_vie.sql")


def rebuild_semantic_layer(cfg: DvfConfig) -> None:
    """sql/semantic.sql (DVF objects) then the housing / living-environment
    views, in ONE transaction under the schema lock: readers never see a
    half-built layer."""
    ddl = "\n".join(
        (cfg.sql_dir / name).read_text(encoding="utf-8") for name in SEMANTIC_FILES
    )
    hook = PostgresHook(postgres_conn_id=cfg.conn_id)
    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(%s)", (SCHEMA_ADVISORY_LOCK_ID,))
            cur.execute(ddl)
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()


def read_downloaded(path: str | Path) -> bytes:
    """Bytes of a file saved by download_file, gunzipped if needed.

    download_file keeps the bytes exactly as served (right for DVF's .csv.gz),
    but static.data.gouv.fr and some APIs send plain .csv with
    Content-Encoding: gzip, so the file on disk is gzip data: detect the
    magic number instead of trusting the file name.
    """
    return ungzip(Path(path).read_bytes())


def ungzip(raw: bytes) -> bytes:
    """Gunzip when the gzip magic number is there, else return the bytes as is."""
    return gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw


# Generic typed parsing, gates and table replacement (several DAGs use them)
def decode(raw: bytes) -> str:
    """UTF-8 (with or without BOM), else Windows-1252."""
    try:
        return raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        return raw.decode("cp1252")


def to_count(value: str) -> int | None:
    """' 26 755    ' -> 26755 (spaces, including non-breaking, are thousands separators)."""
    return to_int(re.sub(r"[\s  ]", "", value or ""))


def to_year(value: str) -> int | None:
    year = to_int(value)
    if year is not None and not 1000 <= year <= 2100:
        raise ValueError(f"implausible year {value!r}")
    return year


Spec = dict[str, tuple[tuple[str, ...], Callable[[str], Any]]]


def iter_typed(
    records: Iterable[dict[str, str]],
    fieldnames: list[str],
    spec: Spec,
    required: set[str],
    stats: LoadStats,
    label: str,
) -> Iterator[list[Any]]:
    """Typed rows from dict records; each target column accepts several source names."""
    names = {name.strip(): name for name in fieldnames}
    plan: list[tuple[str, str | None, Callable[[str], Any]]] = []
    for target, (aliases, convert) in spec.items():
        source = next((names[a] for a in aliases if a in names), None)
        if source is None and target in required:
            raise AirflowFailException(
                f"{label}: schema drift, none of {aliases} found"
            )
        plan.append((target, source, convert))
    for record in records:
        stats.rows_read += 1
        row: list[Any] = []
        try:
            for target, source, convert in plan:
                raw = (record.get(source) or "").strip() if source else ""
                try:
                    value = convert(raw)
                except ValueError:
                    if target in required:
                        raise
                    stats.nulled[target] += 1
                    value = None
                if value is None and target in required:
                    raise ValueError(f"{target} is required")
                row.append(value)
        except ValueError as exc:
            stats.rows_rejected += 1
            if stats.rows_rejected <= 10:
                log.warning("%s: rejected row: %s", label, exc)
            continue
        stats.rows_loaded += 1
        yield row


def check_gates(stats: LoadStats, min_rows: int, max_ratio: float, label: str) -> None:
    if stats.rows_loaded < min_rows:
        raise AirflowFailException(
            f"{label}: only {stats.rows_loaded:,} rows (expected >= {min_rows:,})"
        )
    if stats.reject_ratio > max_ratio:
        raise AirflowFailException(
            f"{label}: reject ratio {stats.reject_ratio:.4%} "
            f"({stats.rows_rejected:,} / {stats.rows_read:,})"
        )
    check_format(stats, max_ratio)


def replace_rows(
    cfg: DvfConfig,
    table: str,
    columns: list[str],
    rows: Iterable[list[Any]],
    where: str = "",
    params: tuple = (),
) -> None:
    """DELETE (all, or `where`) + COPY, in one transaction under the schema lock."""
    buffer = io.StringIO()
    csv.writer(buffer, lineterminator="\n").writerows(rows)
    hook = PostgresHook(postgres_conn_id=cfg.conn_id)
    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(%s)", (SCHEMA_ADVISORY_LOCK_ID,))
            cur.execute(f"DELETE FROM {table} {where}", params)
            copy_buffer(
                cur,
                f"COPY {table} ({', '.join(columns)}) FROM STDIN WITH (FORMAT csv)",
                buffer,
            )
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()
