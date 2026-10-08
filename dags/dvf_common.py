"""Shared settings for the DVF DAGs. This module defines NO DAG.

The DAG files import from here and never from each other. If one DAG file
imported another, the imported file would run again in the dag-processor and
register its DAG twice (AirflowDagDuplicatedIdException). It plays the role of
``config.py`` in datagouvfr_data_pipelines, and it's listed in .airflowignore
so the dag-processor doesn't parse it on its own.
"""

from dataclasses import dataclass
from pathlib import Path

from airflow.sdk import Variable

LOADER_DAG_ID = "dvf_postgres_pipeline"
DETECTOR_DAG_ID = "dvf_change_detector"

# 2014 is the first DVF year ever published. /latest only serves the last 5 years.
MIN_YEAR = 2014

HTTP_TIMEOUT = (10, 300)  # (connect, read) seconds


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
        )

    def source_url(self, year: int) -> str:
        return f"{self.base_url}/{year}/full.csv.gz"

    def local_path(self, year: int) -> Path:
        return self.data_dir / str(year) / "full.csv.gz"
