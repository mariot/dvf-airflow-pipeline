# Official Airflow image + the few libraries our DAGs need on top of it.
# Keep the tag in sync with pyproject.toml and the constraints URL.
FROM apache/airflow:3.3.2-python3.14

# xlrd reads the legacy .xls workbook of the long series (dvf_logement_pipeline).
# Not in Airflow's constraints file, so pinned here.
RUN pip install --no-cache-dir "xlrd==2.0.2"
