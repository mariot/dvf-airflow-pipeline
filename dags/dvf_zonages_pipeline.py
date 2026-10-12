"""Zonings inside communes: quartiers and IRIS, so sales can be located in them.

    sync_schema ──┬─> load_quartiers ──┐
                  └─> load_iris ───────┴─> refresh_semantic_layer

* load_quartiers: Bordeaux Métropole's "Quartier des communes" (Licence
  Ouverte): the quartiers of the communes that divided their territory, mostly
  political quartiers with an elected representative (Bordeaux: 8, from
  Bordeaux maritime to La Bastide; Pessac, Talence, Mérignac, Bègles...). Only
  part of the métropole is covered, and the publisher says the dataset is
  "en cours de construction": some objects are not validated yet (valide).
* load_iris: Contours...IRIS (IGN / INSEE, Licence Ouverte) from the
  Géoplateforme WFS, for every department of DVF_SCOPE_DEPARTEMENTS. Communes
  that are not divided are one IRIS of type 'Z'. IRIS names are numbered
  ("Chartrons-Grand Parc 3"): they are statistical units, not the names
  people use.
* refresh_semantic_layer: dvf.ventes and dvf.biens get each sale's quartier and
  IRIS (point in polygon, same commune), so the semantic layer is rebuilt.

Both sources are GeoJSON in WGS84 (longitude, latitude); polygons are repaired
(ST_MakeValid) and stored in Lambert-93 like every other geometry.
"""

import csv
import io
import json
import logging
from datetime import UTC, datetime, timedelta
from typing import Any

import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task
from airflow.sdk.exceptions import AirflowFailException
from dvf_io import (
    SCHEMA_ADVISORY_LOCK_ID,
    apply_sql_file,
    copy_buffer,
    rebuild_semantic_layer,
)

from dvf_common import HTTP_TIMEOUT, ZONAGES_DAG_ID, DvfConfig

log = logging.getLogger(__name__)

MIN_QUARTIERS = 20
MIN_IRIS_PER_DEPARTEMENT = 100
POLYGON_TYPES = frozenset({"Polygon", "MultiPolygon"})
# WGS84 GeoJSON -> valid Lambert-93 MultiPolygon (MakeValid can return a
# collection with lines: keep the polygons only).
TO_L93 = (
    "ST_Multi(ST_CollectionExtract(ST_MakeValid("
    "ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(geojson), 4326), 2154)), 3))"
)


def to_bool(value: Any) -> bool | None:
    """The datahub writes booleans as 'True' / 'False' strings."""
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    return {"true": True, "false": False}.get(str(value).strip().lower())


def quartier_rows(features: list[dict]) -> tuple[list[list[Any]], int]:
    """Rows of dvf.ref_quartiers from the GeoJSON features, and the rejects."""
    rows, rejected = [], 0
    for feature in features:
        props, geometry = feature.get("properties") or {}, feature.get("geometry")
        code = str(props.get("insee") or "").strip()
        nom = str(props.get("nom") or "").strip()
        if (
            props.get("gid") is None
            or len(code) != 5
            or not nom
            or not geometry
            or geometry.get("type") not in POLYGON_TYPES
        ):
            rejected += 1
            continue
        rows.append(
            [
                int(props["gid"]),
                code,
                nom,
                to_bool(props.get("quarpoli")),
                to_bool(props.get("valide")),
                props.get("mdate") or props.get("cdate"),
                json.dumps(geometry),
            ]
        )
    return rows, rejected


def iris_rows(features: list[dict], departement: str) -> tuple[list[list[Any]], int]:
    """Rows of dvf.ref_iris for one department, and the rejects."""
    rows, rejected = [], 0
    for feature in features:
        props, geometry = feature.get("properties") or {}, feature.get("geometry")
        code_iris = str(props.get("code_iris") or "").strip()
        code = str(props.get("code_insee") or "").strip()
        if (
            len(code_iris) != 9
            or not code.startswith(departement)
            or code_iris[:5] != code
            or not props.get("nom_iris")
            or not geometry
            or geometry.get("type") not in POLYGON_TYPES
        ):
            rejected += 1
            continue
        rows.append(
            [
                code_iris,
                code,
                props.get("nom_commune"),
                str(props["nom_iris"]).strip(),
                (props.get("type_iris") or None),
                json.dumps(geometry),
            ]
        )
    return rows, rejected


def fetch_iris(url_template: str, departement: str) -> list[dict]:
    """Every IRIS feature of one department, page by page (WFS STARTINDEX)."""
    features: list[dict] = []
    while True:
        url = url_template.format(dep=departement, start=len(features))
        response = requests.get(url, timeout=HTTP_TIMEOUT)
        response.raise_for_status()
        page = response.json()
        batch = page.get("features") or []
        features.extend(batch)
        matched = page.get("numberMatched")
        if not batch or (isinstance(matched, int) and len(features) >= matched):
            return features


def check_rejects(label: str, loaded: int, rejected: int, minimum: int) -> None:
    if loaded < minimum:
        raise AirflowFailException(
            f"{label}: only {loaded} rows (expected >= {minimum})"
        )
    if rejected > max(2, loaded // 100):
        raise AirflowFailException(
            f"{label}: {rejected} rejected features for {loaded}"
        )


def replace_polygons(
    cfg: DvfConfig, table: str, columns: list[str], rows: list[list[Any]]
) -> None:
    """Replace the whole table: COPY into a staging table (geometry as GeoJSON
    text, last column), convert to Lambert-93, in one transaction.

    The staging columns take the target's types, so COPY parses the values
    (gid, booleans, dates): a TEXT staging column can't be inserted into an
    INTEGER one."""
    names = ", ".join(columns)
    hook = PostgresHook(postgres_conn_id=cfg.conn_id)
    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(%s)", (SCHEMA_ADVISORY_LOCK_ID,))
            cur.execute(
                f"CREATE TEMP TABLE staging ON COMMIT DROP AS "
                f"SELECT {names} FROM {table} WITH NO DATA"
            )
            cur.execute("ALTER TABLE staging ADD COLUMN geojson TEXT")
            buffer = io.StringIO()
            csv.writer(buffer, lineterminator="\n").writerows(rows)
            copy_buffer(cur, "COPY staging FROM STDIN WITH (FORMAT csv)", buffer)
            cur.execute(f"TRUNCATE {table}")
            cur.execute(
                f"INSERT INTO {table} ({names}, geom) "
                f"SELECT {names}, {TO_L93} FROM staging"
            )
            cur.execute(
                f"SELECT count(*), count(*) FILTER (WHERE ST_IsEmpty(geom)) FROM {table}"
            )
            total, empty = cur.fetchone()
            if empty:
                raise AirflowFailException(
                    f"{table}: {empty} of {total} polygons are empty"
                )
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()


@dag(
    dag_id=ZONAGES_DAG_ID,
    description="Quartiers and IRIS, and each sale's quartier / IRIS",
    schedule="0 7 5 * *",  # monthly: the quartiers are still being validated
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["data_processing", "dvf", "zonages", "postgis"],
    doc_md=__doc__,
)
def dvf_zonages_pipeline() -> None:
    @task(execution_timeout=timedelta(minutes=10))
    def sync_schema() -> None:
        apply_sql_file(DvfConfig.from_airflow(), "init.sql")

    @task(execution_timeout=timedelta(minutes=10))
    def load_quartiers() -> dict[str, Any]:
        cfg = DvfConfig.from_airflow()
        response = requests.get(cfg.quartiers_url, timeout=HTTP_TIMEOUT)
        response.raise_for_status()
        rows, rejected = quartier_rows(response.json().get("features") or [])
        check_rejects("quartiers", len(rows), rejected, MIN_QUARTIERS)
        if len({r[0] for r in rows}) != len(rows):
            raise AirflowFailException("quartiers: duplicate gid")
        replace_polygons(
            cfg,
            "dvf.ref_quartiers",
            [
                "gid",
                "code_commune",
                "nom",
                "quartier_politique",
                "valide",
                "modifie_le",
            ],
            rows,
        )
        communes = sorted({r[1] for r in rows})
        log.info(
            "%s quartiers in %s communes (%s rejected)",
            len(rows),
            len(communes),
            rejected,
        )
        return {"quartiers": len(rows), "communes": len(communes), "rejected": rejected}

    @task(execution_timeout=timedelta(minutes=20))
    def load_iris() -> dict[str, Any]:
        cfg = DvfConfig.from_airflow()
        rows: list[list[Any]] = []
        for dep in cfg.scope_departements:
            dep_rows, rejected = iris_rows(fetch_iris(cfg.iris_wfs_url, dep), dep)
            check_rejects(
                f"IRIS {dep}", len(dep_rows), rejected, MIN_IRIS_PER_DEPARTEMENT
            )
            rows.extend(dep_rows)
        if len({r[0] for r in rows}) != len(rows):
            raise AirflowFailException("IRIS: duplicate code_iris")
        replace_polygons(
            cfg,
            "dvf.ref_iris",
            ["code_iris", "code_commune", "nom_commune", "nom_iris", "type_iris"],
            rows,
        )
        log.info("%s IRIS for %s", len(rows), cfg.scope_departements)
        return {"iris": len(rows)}

    @task(execution_timeout=timedelta(minutes=30))
    def refresh_semantic_layer(quartiers: dict[str, Any], iris: dict[str, Any]) -> None:
        """dvf.ventes / dvf.biens carry the zonings: rebuild them (and the views)."""
        rebuild_semantic_layer(DvfConfig.from_airflow())
        log.info("Semantic layer rebuilt with %s and %s", quartiers, iris)

    schema = sync_schema()
    quartiers, iris = load_quartiers(), load_iris()
    schema >> [quartiers, iris]
    refresh_semantic_layer(quartiers, iris)


dvf_zonages_pipeline()
