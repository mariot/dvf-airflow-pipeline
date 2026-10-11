"""Accessibility for a home search: walking graph, schools, transport, addresses.

    sync_schema ──> load_contours ──┬─> load_walking_graph ──┐
                                    ├─> load_schools ────────┤
                                    ├─> load_addresses ──────┼─> compute_access
                                    └─> load_transport ──────┘

Scope: DVF_SCOPE_DEPARTEMENTS (default '33', Gironde).

* load_contours: commune outlines (geo.api.gouv.fr); their union is the scope.
* load_walking_graph: OpenStreetMap (Geofabrik extract) -> dvf.osm_pieton, a
  pgRouting graph whose vertices are OSM node ids; cost = walking seconds at
  4.5 km/h (steps count double). Connected components are computed; only
  vertices of components with >= 1 000 vertices are used for snapping.
* load_schools: annuaire de l'éducation (écoles, collèges, lycées), social
  position indices (IPS) and collège value-added indicators (IVAC).
* load_addresses: Base Adresse Nationale, with their cadastral parcels (the
  same ids as DVF's id_parcelle).
* load_transport: GTFS feeds (DVF_GTFS_FEEDS): stops, and per stop the time
  to each pole of dvf.acces_poles for an arrival between 07:30 and 09:00 on a
  reference Tuesday (see dvf_transit: best and typical minutes).
* compute_access, in pgRouting:
    - snap schools, stops and addresses to the nearest usable vertex;
    - nearest school of each level by walking: one multi-source
      pgr_drivingDistance(equicost) per level, capped at 30 minutes;
    - door-to-pole time: a virtual vertex linked to every stop (cost = its
      typical transit time) and to the pole's vertex (cost 0, walking only);
      one pgr_drivingDistance from it gives, for every vertex,
      min(walk to a stop + transit, walk all the way), capped at 90 minutes.

Address-level results: the view dvf.v_acces_adresses adds the short walk
from the address to its vertex.
"""

import csv
import io
import json
import logging
import urllib.parse
import zipfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import dvf_osm
import dvf_transit
import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task
from airflow.sdk.exceptions import AirflowFailException
from dvf_io import (
    SCHEMA_ADVISORY_LOCK_ID,
    LoadStats,
    Spec,
    apply_sql_file,
    check_gates,
    copy_buffer,
    decode,
    download_file,
    iter_typed,
    read_downloaded,
    to_code_commune,
    to_decimal,
    to_int,
    to_text,
)

from dvf_common import ACCES_DAG_ID, HTTP_TIMEOUT, DvfConfig

log = logging.getLogger(__name__)

GEO_API = "https://geo.api.gouv.fr/departements/{dep}/communes?format=geojson&geometry=contour"
EDU_API = (
    "https://data.education.gouv.fr/api/explore/v2.1/catalog/datasets/{ds}/exports/csv"
)
BAN_CSV = (
    "https://adresse.data.gouv.fr/data/ban/adresses/latest/csv/adresses-{dep}.csv.gz"
)
SNAP_MIN_COMPONENT = 1_000  # vertices: smaller islands of the graph aren't snapped to
SCHOOL_MAX_S = 30 * 60
POLE_MAX_S = 90 * 60


def run_sql(
    cfg: DvfConfig, statements: list[str], params: list[tuple] | None = None
) -> None:
    hook = PostgresHook(postgres_conn_id=cfg.conn_id)
    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(%s)", (SCHEMA_ADVISORY_LOCK_ID,))
            for i, sql in enumerate(statements):
                cur.execute(sql, (params[i] if params else None))
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()


def edu_where(field: str, departements: tuple[str, ...]) -> str:
    """ODSQL filter accepting both '33' and '033' (the datasets disagree)."""
    values = {d for dep in departements for d in (dep, dep.zfill(3))}
    return " OR ".join(f'{field}="{v}"' for v in sorted(values))


def edu_url(dataset: str, field: str, departements: tuple[str, ...]) -> str:
    where = urllib.parse.quote(edu_where(field, departements))
    return f"{EDU_API.format(ds=dataset)}?where={where}"


SUPPRESSED = frozenset(
    {"NS", "ND", "NC", "S", "-"}
)  # non significatif / non diffusé / secret


def to_indicator(value: str):
    """Decimal, or None for the ministry's suppression markers (small schools)."""
    if value.strip().upper() in SUPPRESSED:
        return None
    return to_decimal(value)


def to_flag(value: str) -> bool | None:
    return {"1": True, "0": False}.get(value.strip()) if value else None


def school_type(value: str) -> str | None:
    return {"Ecole": "ecole", "Collège": "college", "Lycée": "lycee"}.get(value.strip())


SCHOOL_SPEC: Spec = {
    "uai": (("identifiant_de_l_etablissement",), to_text),
    "nom": (("nom_etablissement",), to_text),
    "type": (("type_etablissement",), school_type),
    "maternelle": (("ecole_maternelle",), to_flag),
    "elementaire": (("ecole_elementaire",), to_flag),
    "lycee_general": (("voie_generale",), to_flag),
    "statut": (("statut_public_prive",), to_text),
    "education_prioritaire": (("appartenance_education_prioritaire",), to_text),
    "code_commune": (("code_commune",), to_code_commune),
    "nom_commune": (("nom_commune",), to_text),
    "adresse": (("adresse_1",), to_text),
    "code_nature": (("code_nature",), to_text),
    "nature": (("libelle_nature",), to_text),
    "longitude": (("longitude",), to_decimal),
    "latitude": (("latitude",), to_decimal),
}
IPS_SPEC: Spec = {
    "uai": (("uai",), to_text),
    "rentree": (("rentree_scolaire",), to_text),
    "ips": (("ips",), to_indicator),
    "ips_departemental": (("ips_departemental",), to_indicator),
    "ecart_type": (("ecart_type_de_l_ips",), to_indicator),
}
IVAC_SPEC: Spec = {
    "uai": (("uai",), to_text),
    "session": (("session",), lambda v: to_int(v[:4]) if v else None),
    "nb_candidats": (("nb_candidats_g",), to_int),
    "taux_reussite": (("taux_de_reussite_g",), to_indicator),
    "va_taux_reussite": (("va_du_taux_de_reussite_g",), to_indicator),
    "note_ecrit": (("note_a_l_ecrit_g",), to_indicator),
    "va_note_ecrit": (("va_de_la_note_g",), to_indicator),
    "taux_acces_6e_3e": (("taux_d_acces_6eme_3eme",), to_indicator),
}
BAN_SPEC: Spec = {
    "id": (("id",), to_text),
    "numero": (("numero",), to_int),
    "rep": (("rep",), to_text),
    "nom_voie": (("nom_voie",), to_text),
    "code_postal": (("code_postal",), to_text),
    "code_commune": (("code_insee",), to_code_commune),
    "nom_commune": (("nom_commune",), to_text),
    "parcelles": (
        ("cad_parcelles",),
        lambda v: "{" + v.replace("|", ",") + "}" if v else None,
    ),
    "x": (("x",), to_decimal),
    "y": (("y",), to_decimal),
}


def csv_rows(text: str, spec: Spec, required: set[str], stats: LoadStats, label: str):
    reader = csv.DictReader(io.StringIO(text, newline=""), delimiter=";")
    return iter_typed(reader, reader.fieldnames or [], spec, required, stats, label)


@dag(
    dag_id=ACCES_DAG_ID,
    description="Walking graph, schools, public transport and addresses for a home search",
    schedule="0 8 4 * *",  # monthly; GTFS change more often, rerun on demand
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["data_processing", "dvf", "accessibilite", "pgrouting"],
    doc_md=__doc__,
)
def dvf_accessibilite_pipeline() -> None:
    @task(execution_timeout=timedelta(minutes=10))
    def sync_schema() -> None:
        apply_sql_file(DvfConfig.from_airflow(), "init.sql")

    @task(execution_timeout=timedelta(minutes=10))
    def load_contours() -> list[float]:
        """Commune outlines of the scope; returns its WGS84 bbox (+ ~3 km)."""
        cfg = DvfConfig.from_airflow()
        rows = []
        for dep in cfg.scope_departements:
            response = requests.get(GEO_API.format(dep=dep), timeout=HTTP_TIMEOUT)
            response.raise_for_status()
            for feature in response.json()["features"]:
                props = feature["properties"]
                rows.append(
                    [props["code"], props["nom"], dep, json.dumps(feature["geometry"])]
                )
        if len(rows) < 10:
            raise AirflowFailException(
                f"Only {len(rows)} commune outlines for {cfg.scope_departements}"
            )
        hook = PostgresHook(postgres_conn_id=cfg.conn_id)
        conn = hook.get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_advisory_xact_lock(%s)", (SCHEMA_ADVISORY_LOCK_ID,)
                )
                cur.execute(
                    "CREATE TEMP TABLE contours_raw (code TEXT, nom TEXT, dep TEXT, geojson TEXT) ON COMMIT DROP"
                )
                buffer = io.StringIO()
                csv.writer(buffer, lineterminator="\n").writerows(rows)
                copy_buffer(
                    cur, "COPY contours_raw FROM STDIN WITH (FORMAT csv)", buffer
                )
                cur.execute("TRUNCATE dvf.ref_communes_contours")
                cur.execute("""
                    INSERT INTO dvf.ref_communes_contours (code_commune, nom, dep_code, geom)
                    SELECT code, nom, dep,
                           ST_Multi(ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(geojson), 4326), 2154))
                    FROM contours_raw
                """)
                cur.execute("""
                    SELECT ST_XMin(b), ST_YMin(b), ST_XMax(b), ST_YMax(b)
                    FROM (SELECT ST_Transform(ST_SetSRID(ST_Expand(ST_Extent(geom)::geometry, 3000), 2154), 4326) AS b
                          FROM dvf.ref_communes_contours) t
                """)
                bbox = [float(v) for v in cur.fetchone()]
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()
        log.info("Scope: %s communes, bbox %s", len(rows), bbox)
        return bbox

    @task(execution_timeout=timedelta(hours=1))
    def load_walking_graph(bbox: list[float]) -> dict[str, Any]:
        cfg = DvfConfig.from_airflow()
        source = download_file(
            cfg.osm_pbf_url, cfg.reference_path("osm", Path(cfg.osm_pbf_url).name)
        )
        ways = list(dvf_osm.read_walkable_ways(source["path"], tuple(bbox)))
        buffer = io.StringIO()
        writer = csv.writer(buffer, lineterminator="\n")
        count = 0
        for seg in dvf_osm.split_at_junctions(ways):
            count += 1
            writer.writerow(
                [
                    count,
                    seg.osm_id,
                    seg.source,
                    seg.target,
                    seg.highway,
                    dvf_osm.wkt_linestring(seg.coords),
                ]
            )
        del ways
        if count < 10_000:
            raise AirflowFailException(f"Walking graph: only {count:,} segments")
        hook = PostgresHook(postgres_conn_id=cfg.conn_id)
        conn = hook.get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_advisory_xact_lock(%s)", (SCHEMA_ADVISORY_LOCK_ID,)
                )
                cur.execute("""CREATE TEMP TABLE seg_raw (id BIGINT, osm_id BIGINT, source BIGINT,
                               target BIGINT, highway TEXT, wkt TEXT) ON COMMIT DROP""")
                copy_buffer(cur, "COPY seg_raw FROM STDIN WITH (FORMAT csv)", buffer)
                cur.execute("TRUNCATE dvf.osm_pieton, dvf.osm_pieton_noeuds")
                # keep segments within 2 km of the scope (walks may cross its border)
                cur.execute("""
                    INSERT INTO dvf.osm_pieton (id, osm_id, source, target, highway, longueur_m, cout_s, geom)
                    SELECT id, osm_id, source, target, highway, ST_Length(g),
                           ST_Length(g) / 1.25 * CASE WHEN highway = 'steps' THEN 2 ELSE 1 END, g
                    FROM (SELECT s.*, ST_Transform(ST_GeomFromText(wkt, 4326), 2154) AS g FROM seg_raw s) t
                    WHERE EXISTS (SELECT 1 FROM dvf.ref_communes_contours c
                                  WHERE ST_DWithin(c.geom, t.g, 2000))
                """)
                cur.execute("""
                    INSERT INTO dvf.osm_pieton_noeuds (id, geom)
                    SELECT DISTINCT ON (id) id, geom FROM (
                        SELECT source AS id, ST_StartPoint(geom) AS geom FROM dvf.osm_pieton
                        UNION ALL
                        SELECT target, ST_EndPoint(geom) FROM dvf.osm_pieton) v
                    ORDER BY id
                """)
                cur.execute(
                    """
                    WITH comp AS (
                        SELECT node, component FROM pgr_connectedComponents(
                            'SELECT id, source, target, cout_s AS cost, cout_s AS reverse_cost FROM dvf.osm_pieton')
                    ), sizes AS (SELECT component, count(*) AS n FROM comp GROUP BY component)
                    UPDATE dvf.osm_pieton_noeuds v
                    SET composante = c.component, principal = s.n >= %s
                    FROM comp c JOIN sizes s USING (component)
                    WHERE v.id = c.node
                """,
                    (SNAP_MIN_COMPONENT,),
                )
                cur.execute(
                    "SELECT count(*), count(*) FILTER (WHERE principal) FROM dvf.osm_pieton_noeuds"
                )
                vertices, usable = cur.fetchone()
                cur.execute("SELECT count(*) FROM dvf.osm_pieton")
                (edges,) = cur.fetchone()
                cur.execute("ANALYZE dvf.osm_pieton; ANALYZE dvf.osm_pieton_noeuds")
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()
        log.info(
            "Walking graph: %s edges, %s vertices (%s snappable)",
            edges,
            vertices,
            usable,
        )
        return {"edges": edges, "vertices": vertices, "snappable": usable}

    @task(execution_timeout=timedelta(minutes=20))
    def load_schools(bbox: list[float]) -> dict[str, Any]:
        cfg = DvfConfig.from_airflow()
        deps = cfg.scope_departements

        def fetch(dataset: str, field: str) -> str:
            url = edu_url(dataset, field, deps)
            path = cfg.reference_path("ecoles", f"{dataset}.csv")
            return decode(read_downloaded(download_file(url, path)["path"]))

        stats = LoadStats()
        schools = [
            r
            for r in csv_rows(
                fetch("fr-en-annuaire-education", "code_departement"),
                SCHOOL_SPEC,
                {"uai", "code_commune"},
                stats,
                "Annuaire",
            )
            if r[2] is not None
        ]  # écoles, collèges, lycées only
        check_gates(stats, 50, cfg.max_reject_ratio, "Annuaire")
        ips: list[list[Any]] = []
        for dataset, niveau in (
            ("fr-en-ips-ecoles-ap2022", "ecole"),
            ("fr-en-ips-colleges-ap2022", "college"),
            ("fr-en-ips-colleges-ap2023", "college"),
        ):
            s = LoadStats()
            ips += [
                [*r[:2], niveau, *r[2:]]
                for r in csv_rows(
                    fetch(dataset, "code_du_departement"),
                    IPS_SPEC,
                    {"uai", "rentree"},
                    s,
                    dataset,
                )
            ]
            check_gates(s, 10, cfg.max_reject_ratio, dataset)
        s = LoadStats()
        ivac = list(
            csv_rows(
                fetch("fr-en-indicateurs-valeur-ajoutee-colleges", "code_departement"),
                IVAC_SPEC,
                {"uai", "session"},
                s,
                "IVAC",
            )
        )
        check_gates(s, 10, cfg.max_reject_ratio, "IVAC")

        school_cols = [c for c in SCHOOL_SPEC if c not in ("longitude", "latitude")]
        lon, lat = (
            list(SCHOOL_SPEC).index("longitude"),
            list(SCHOOL_SPEC).index("latitude"),
        )
        rows = [
            [
                *(v for i, v in enumerate(r) if i not in (lon, lat)),
                f"SRID=4326;POINT({r[lon]} {r[lat]})" if r[lon] is not None else None,
            ]
            for r in schools
        ]
        hook = PostgresHook(postgres_conn_id=cfg.conn_id)
        conn = hook.get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_advisory_xact_lock(%s)", (SCHEMA_ADVISORY_LOCK_ID,)
                )
                cur.execute(
                    "TRUNCATE dvf.ref_ecoles, dvf.ref_ecoles_ips, dvf.ref_colleges_ivac"
                )
                cur.execute(
                    f"CREATE TEMP TABLE ecoles_raw ({', '.join(c + ' TEXT' for c in school_cols)}, ewkt TEXT) ON COMMIT DROP"
                )
                buffer = io.StringIO()
                csv.writer(buffer, lineterminator="\n").writerows(rows)
                copy_buffer(cur, "COPY ecoles_raw FROM STDIN WITH (FORMAT csv)", buffer)
                cur.execute(f"""
                    INSERT INTO dvf.ref_ecoles ({", ".join(school_cols)}, geom)
                    SELECT DISTINCT ON (uai) uai, nom, type, maternelle::boolean, elementaire::boolean,
                           lycee_general::boolean, statut, education_prioritaire, code_commune,
                           nom_commune, adresse, code_nature, nature,
                           ST_Transform(ST_GeomFromEWKT(ewkt), 2154)
                    FROM ecoles_raw ORDER BY uai
                """)
                for table, cols, data in (
                    (
                        "dvf.ref_ecoles_ips",
                        [
                            "uai",
                            "rentree",
                            "niveau",
                            "ips",
                            "ips_departemental",
                            "ecart_type",
                        ],
                        ips,
                    ),
                    ("dvf.ref_colleges_ivac", list(IVAC_SPEC), ivac),
                ):
                    buffer = io.StringIO()
                    csv.writer(buffer, lineterminator="\n").writerows(data)
                    copy_buffer(
                        cur,
                        f"COPY {table} ({', '.join(cols)}) FROM STDIN WITH (FORMAT csv)",
                        buffer,
                    )
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()
        return {"schools": len(rows), "ips": len(ips), "ivac": len(ivac)}

    @task(execution_timeout=timedelta(minutes=30))
    def load_addresses(bbox: list[float]) -> dict[str, Any]:
        cfg = DvfConfig.from_airflow()
        stats = LoadStats()
        rows: list[list[Any]] = []
        for dep in cfg.scope_departements:
            url = BAN_CSV.format(dep=dep)
            source = download_file(url, cfg.reference_path("ban", Path(url).name))
            rows += csv_rows(
                decode(read_downloaded(source["path"])),
                BAN_SPEC,
                {"id", "code_commune", "x", "y"},
                stats,
                f"BAN {dep}",
            )
        check_gates(stats, 1_000, cfg.max_reject_ratio, "BAN")
        hook = PostgresHook(postgres_conn_id=cfg.conn_id)
        conn = hook.get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_advisory_xact_lock(%s)", (SCHEMA_ADVISORY_LOCK_ID,)
                )
                cur.execute("""CREATE TEMP TABLE ban_raw (id TEXT, numero INTEGER, rep TEXT, nom_voie TEXT,
                               code_postal TEXT, code_commune TEXT, nom_commune TEXT, parcelles TEXT[],
                               x DOUBLE PRECISION, y DOUBLE PRECISION) ON COMMIT DROP""")
                buffer = io.StringIO()
                csv.writer(buffer, lineterminator="\n").writerows(rows)
                copy_buffer(cur, "COPY ban_raw FROM STDIN WITH (FORMAT csv)", buffer)
                cur.execute("TRUNCATE dvf.ref_adresses")
                cur.execute("""
                    INSERT INTO dvf.ref_adresses (id, numero, rep, nom_voie, code_postal, code_commune,
                                                  nom_commune, parcelles, geom)
                    SELECT DISTINCT ON (id) id, numero, rep, nom_voie, code_postal, code_commune,
                           nom_commune, parcelles, ST_SetSRID(ST_MakePoint(x, y), 2154)
                    FROM ban_raw ORDER BY id
                """)
                cur.execute("ANALYZE dvf.ref_adresses")
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()
        return {"addresses": stats.rows_loaded}

    @task(execution_timeout=timedelta(minutes=45))
    def load_transport(bbox: list[float]) -> dict[str, Any]:
        cfg = DvfConfig.from_airflow()
        west, south, east, north = bbox
        keep = lambda lat, lon: south <= lat <= north and west <= lon <= east
        day = dvf_transit.reference_day(datetime.now(tz=UTC).date())
        hook = PostgresHook(postgres_conn_id=cfg.conn_id)
        poles = hook.get_records("""
            SELECT pole, ST_Y(ST_Transform(geom, 4326)), ST_X(ST_Transform(geom, 4326)),
                   extract(epoch FROM arrivee_debut)::int, extract(epoch FROM arrivee_fin)::int
            FROM dvf.acces_poles ORDER BY pole
        """)
        earliest = min(p[3] for p in poles) - 3 * 3600
        latest = max(p[4] for p in poles)
        stops: dict[str, dvf_transit.Stop] = {}
        connections: list[dvf_transit.Connection] = []
        modes: dict[str, set[str]] = {}
        for network, url in cfg.gtfs_feeds:
            source = download_file(url, cfg.reference_path("gtfs", f"{network}.zip"))
            with zipfile.ZipFile(io.BytesIO(read_downloaded(source["path"]))) as zf:
                feed_stops = dvf_transit.read_stops(zf, network, keep)
                feed_conns, feed_modes = dvf_transit.read_connections(
                    zf, network, day, feed_stops, (earliest, latest)
                )
            if not feed_conns:
                raise AirflowFailException(f"GTFS {network}: no connection on {day}")
            log.info(
                "GTFS %s: %s stops, %s connections on %s",
                network,
                len(feed_stops),
                len(feed_conns),
                day,
            )
            stops.update(feed_stops)
            connections += feed_conns
            modes.update(feed_modes)
        paths = dvf_transit.footpaths(stops)
        times: list[list[Any]] = []
        for pole, lat, lon, start, end in poles:
            access = {
                key: dvf_transit.walk_seconds(dvf_transit.distance_m(s, lat, lon))
                for key, s in stops.items()
                if dvf_transit.distance_m(s, lat, lon) <= dvf_transit.MAX_ACCESS_M
            }
            result = dvf_transit.travel_minutes(
                connections, paths, access, range(start, end + 1, 120)
            )
            times += [
                [stop, pole, best, typical, day.isoformat()]
                for stop, (best, typical) in result.items()
            ]
        stop_rows = [
            [
                s.key,
                s.key.split(":", 1)[0],
                s.name,
                "{" + ",".join(sorted(modes.get(s.key, ()))) + "}",
                f"SRID=4326;POINT({s.lon} {s.lat})",
            ]
            for s in stops.values()
        ]
        conn = hook.get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_advisory_xact_lock(%s)", (SCHEMA_ADVISORY_LOCK_ID,)
                )
                cur.execute("TRUNCATE dvf.transport_arrets, dvf.transport_temps_pole")
                cur.execute(
                    "CREATE TEMP TABLE arrets_raw (arret TEXT, reseau TEXT, nom TEXT, modes TEXT[], ewkt TEXT) ON COMMIT DROP"
                )
                buffer = io.StringIO()
                csv.writer(buffer, lineterminator="\n").writerows(stop_rows)
                copy_buffer(cur, "COPY arrets_raw FROM STDIN WITH (FORMAT csv)", buffer)
                cur.execute("""INSERT INTO dvf.transport_arrets (arret, reseau, nom, modes, geom)
                               SELECT arret, reseau, nom, modes, ST_Transform(ST_GeomFromEWKT(ewkt), 2154)
                               FROM arrets_raw""")
                buffer = io.StringIO()
                csv.writer(buffer, lineterminator="\n").writerows(times)
                copy_buffer(
                    cur,
                    "COPY dvf.transport_temps_pole (arret, pole, minutes_meilleur, minutes_typique, jour_reference) FROM STDIN WITH (FORMAT csv)",
                    buffer,
                )
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()
        return {
            "stops": len(stops),
            "connections": len(connections),
            "times": len(times),
            "day": day.isoformat(),
        }

    @task(execution_timeout=timedelta(hours=2))
    def compute_access(
        graph: dict[str, Any],
        schools: dict[str, Any],
        addresses: dict[str, Any],
        transport: dict[str, Any],
    ) -> dict[str, Any]:
        """Snap everything to the graph, then the pgRouting searches (see module doc)."""
        cfg = DvfConfig.from_airflow()
        walk_edges = "SELECT id, source, target, cout_s AS cost, cout_s AS reverse_cost FROM dvf.osm_pieton"
        snap = """
            WITH nearest AS (
                SELECT t.{key} AS k, n.id, ST_Distance(t.geom, n.geom) AS d
                FROM {table} t
                CROSS JOIN LATERAL (SELECT v.id, v.geom FROM dvf.osm_pieton_noeuds v
                                    WHERE v.principal ORDER BY v.geom <-> t.geom LIMIT 1) n
                WHERE t.geom IS NOT NULL
            )
            UPDATE {table} t SET noeud = nearest.id {extra}
            FROM nearest WHERE t.{key} = nearest.k
        """
        # What a family means by each level: SEGPA sections (390), schools
        # without pupils (170) and sections attached to a lycée (334, 335) are
        # not choices of their own; "lycée" = general / technological /
        # multi-track (vocational lycées, 320, stay in dvf.ref_ecoles).
        levels = {
            "maternelle": "type = 'ecole' AND maternelle AND code_nature <> '170'",
            "elementaire": "type = 'ecole' AND elementaire AND code_nature <> '170'",
            "college": "type = 'college' AND code_nature = '340'",
            "lycee": "type = 'lycee' AND code_nature IN ('300', '301', '302', '306', '307')",
        }
        statements = [
            snap.format(table="dvf.ref_ecoles", key="uai", extra=""),
            snap.format(table="dvf.transport_arrets", key="arret", extra=""),
            snap.format(
                table="dvf.ref_adresses", key="id", extra=", acces_m = nearest.d"
            ),
            "TRUNCATE dvf.acces_noeuds, dvf.acces_noeuds_poles",
            "INSERT INTO dvf.acces_noeuds (noeud) SELECT id FROM dvf.osm_pieton_noeuds WHERE principal",
        ]
        for level, where in levels.items():
            statements.append(f"""
                WITH dd AS (
                    SELECT node, start_vid, agg_cost FROM pgr_drivingDistance(
                        $q${walk_edges}$q$,
                        (SELECT array_agg(DISTINCT noeud) FROM dvf.ref_ecoles WHERE {where} AND noeud IS NOT NULL),
                        {SCHOOL_MAX_S}, false, true)
                ), pick AS (
                    SELECT DISTINCT ON (noeud) noeud, uai FROM dvf.ref_ecoles
                    WHERE {where} AND noeud IS NOT NULL ORDER BY noeud, statut = 'Public' DESC, uai
                )
                UPDATE dvf.acces_noeuds a
                SET min_{level} = round((dd.agg_cost / 60)::numeric, 1), uai_{level} = pick.uai
                FROM dd JOIN pick ON pick.noeud = dd.start_vid
                WHERE a.noeud = dd.node
            """)
        statements.append(f"""
            INSERT INTO dvf.acces_noeuds_poles (noeud, pole, minutes)
            SELECT dd.node, p.pole, round((dd.agg_cost / 60)::numeric, 1)
            FROM dvf.acces_poles p
            CROSS JOIN LATERAL pgr_drivingDistance(
                format($q$
                    {walk_edges}
                    UNION ALL  -- virtual vertex -1 -> each stop's vertex, cost = transit time
                    SELECT -row_number() OVER (), -1, a.noeud,
                           min(t.minutes_typique * 60 + ST_Distance(a.geom, v.geom) * 1.3 / 1.25)::float8,
                           -1::float8
                    FROM dvf.transport_temps_pole t
                    JOIN dvf.transport_arrets a USING (arret)
                    JOIN dvf.osm_pieton_noeuds v ON v.id = a.noeud
                    WHERE t.pole = %L AND a.noeud IS NOT NULL
                    GROUP BY a.noeud
                    UNION ALL  -- virtual vertex -1 -> the pole's vertex: walking all the way
                    SELECT -1000000000, -1, (SELECT v.id FROM dvf.osm_pieton_noeuds v
                                             WHERE v.principal ORDER BY v.geom <-> %L::geometry LIMIT 1),
                           0::float8, -1::float8
                $q$, p.pole, p.geom),
                -1, {POLE_MAX_S}, true) dd
            WHERE dd.node <> -1
        """)
        statements += ["ANALYZE dvf.acces_noeuds", "ANALYZE dvf.acces_noeuds_poles"]
        run_sql(cfg, statements)
        hook = PostgresHook(postgres_conn_id=cfg.conn_id)
        stats = hook.get_first("""
            SELECT count(*), count(min_elementaire), count(min_college),
                   (SELECT count(*) FROM dvf.acces_noeuds_poles)
            FROM dvf.acces_noeuds
        """)
        log.info(
            "Access: %s vertices, %s with an elementary school <= 30 min, "
            "%s with a collège, %s door-to-pole times",
            *stats,
        )
        return {
            "vertices": stats[0],
            "elementaire": stats[1],
            "college": stats[2],
            "pole": stats[3],
        }

    schema = sync_schema()
    bbox = load_contours()
    schema >> bbox
    compute_access(
        graph=load_walking_graph(bbox),
        schools=load_schools(bbox),
        addresses=load_addresses(bbox),
        transport=load_transport(bbox),
    )


dvf_accessibilite_pipeline()
