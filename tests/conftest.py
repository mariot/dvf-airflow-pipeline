import os
import tempfile

# Keep Airflow from writing airflow.cfg / sqlite files into ~/airflow when
# the DAG module is imported. This must run before anything imports airflow.
os.environ.setdefault("AIRFLOW_HOME", tempfile.mkdtemp(prefix="airflow-home-"))
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "false")
os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "true")
