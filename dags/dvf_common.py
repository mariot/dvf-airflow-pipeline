"""Shared settings for the DVF DAGs. This module defines NO DAG.

The DAG files import from here and never from each other. If one DAG file
imported another, the imported file would run again in the dag-processor and
register its DAG twice (AirflowDagDuplicatedIdException). It plays the role of
``config.py`` in datagouvfr_data_pipelines, and it's listed in .airflowignore
so the dag-processor doesn't parse it on its own.
"""

import json
from dataclasses import dataclass
from pathlib import Path

from airflow.sdk import Variable

LOADER_DAG_ID = "dvf_postgres_pipeline"
DETECTOR_DAG_ID = "dvf_change_detector"
LOYERS_DAG_ID = "dvf_loyers_pipeline"
LOGEMENT_DAG_ID = "dvf_logement_pipeline"
ACCES_DAG_ID = "dvf_accessibilite_pipeline"

# 2014 is the first DVF year ever published. /latest only serves the last 5 years.
MIN_YEAR = 2014

HTTP_TIMEOUT = (10, 300)  # (connect, read) seconds

# "Liste des communes de France" (data.gouv.fr, Licence Ouverte 2.0), 2026 edition.
COMMUNES_URL = (
    "https://static.data.gouv.fr/resources/"
    "communes-et-villes-de-france-en-csv-excel-json-parquet-et-feather/"
    "20260617-160400/communes-france-2026.csv.gz"
)
# LOVAC's dataset name carries its last edition: update it when 2027 comes out.
LOVAC_DATASET = "logements-vacants-du-parc-prive-par-commune-departement-region-france-de-2020-a-2026"
# Walking graph: Geofabrik extract containing the scope departments.
OSM_PBF_URL = "https://download.geofabrik.de/europe/france/aquitaine-latest.osm.pbf"
# Public transport (transport.data.gouv.fr), network name -> GTFS zip.
GTFS_FEEDS = {
    "tbm": "https://bdx.mecatran.com/utw/ws/gtfsfeed/static/bordeaux"
    "?apiKey=opendata-bordeaux-metropole-flux-gtfs-rt",
    "transgironde": "https://www.pigma.org/public/opendata/nouvelle_aquitaine_mobilites/"
    "publication/gironde-aggregated-gtfs.zip",
    "sncf": "https://eu.ftp.opendatasoft.com/sncf/plandata/Export_OpenData_SNCF_GTFS_NewTripId.zip",
}
SERIES_LONGUES_URL = (
    "https://www.igedd.developpement-durable.gouv.fr/IMG/xls/"
    "valeur-immobilier-1800-2020_cle2abd1f.xls"
)


@dataclass(frozen=True, slots=True)
class DvfConfig:
    """Runtime settings, read from Airflow Variables (with safe defaults).

    Like the Etalab ``config.py`` pattern, every setting goes through
    ``Variable.get``. Unlike a module-level read, ``from_airflow()`` is called
    inside the tasks, so the scheduler doesn't hit the metadata DB on every
    DAG parse.
    """

    base_url: str
    data_dir: Path
    sql_dir: Path
    conn_id: str
    chunk_rows: int
    max_reject_ratio: float
    max_shrink_ratio: float
    communes_url: str
    rpls_departements: tuple[str, ...]
    lovac_dataset: str
    series_longues_url: str
    scope_departements: tuple[str, ...]
    osm_pbf_url: str
    gtfs_feeds: tuple[tuple[str, str], ...]

    @classmethod
    def from_airflow(cls) -> DvfConfig:
        return cls(
            base_url=Variable.get(
                "DVF_BASE_URL", default="https://files.data.gouv.fr/geo-dvf/latest/csv"
            ).rstrip("/"),
            data_dir=Path(
                Variable.get("DVF_DATA_DIR", default="/opt/airflow/data/dvf")
            ),
            sql_dir=Path(Variable.get("DVF_SQL_DIR", default="/opt/airflow/sql")),
            conn_id=Variable.get("DVF_POSTGRES_CONN_ID", default="dvf_postgres"),
            chunk_rows=int(Variable.get("DVF_COPY_CHUNK_ROWS", default="50000")),
            max_reject_ratio=float(
                Variable.get("DVF_MAX_REJECT_RATIO", default="0.001")
            ),
            max_shrink_ratio=float(Variable.get("DVF_MAX_SHRINK_RATIO", default="0.2")),
            communes_url=Variable.get("DVF_COMMUNES_URL", default=COMMUNES_URL),
            rpls_departements=tuple(
                d.strip()
                for d in Variable.get("DVF_RPLS_DEPARTEMENTS", default="33").split(",")
                if d.strip()
            ),
            lovac_dataset=Variable.get("DVF_LOVAC_DATASET", default=LOVAC_DATASET),
            series_longues_url=Variable.get(
                "DVF_SERIES_LONGUES_URL", default=SERIES_LONGUES_URL
            ),
            scope_departements=tuple(
                d.strip()
                for d in Variable.get("DVF_SCOPE_DEPARTEMENTS", default="33").split(",")
                if d.strip()
            ),
            osm_pbf_url=Variable.get("DVF_OSM_PBF_URL", default=OSM_PBF_URL),
            gtfs_feeds=tuple(
                (name, url)
                for name, url in json.loads(
                    Variable.get("DVF_GTFS_FEEDS", default=json.dumps(GTFS_FEEDS))
                ).items()
            ),
        )

    def source_url(self, year: int) -> str:
        return f"{self.base_url}/{year}/full.csv.gz"

    def local_path(self, year: int) -> Path:
        return self.data_dir / str(year) / "full.csv.gz"

    def reference_path(self, kind: str, name: str) -> Path:
        return self.data_dir / kind / name

    def loyers_path(self, year: int, url: str) -> Path:
        return self.data_dir / "loyers" / str(year) / Path(url).name

    @property
    def communes_path(self) -> Path:
        return self.data_dir / "reference" / Path(self.communes_url).name
