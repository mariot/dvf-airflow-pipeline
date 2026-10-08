# dvf-airflow-pipeline

Loads **DVF géolocalisées** (Demandes de Valeurs Foncières, published by Etalab on
data.gouv.fr) into **PostgreSQL 16** with **Apache Airflow 3.3.2** on
**Python 3.14**. The pipeline is idempotent, keeps memory flat, and detects
when the source files change.

Patterns come from [`datagouv/datagouvfr_data_pipelines`](https://github.com/datagouv/datagouvfr_data_pipelines):
the Airflow 3 `airflow.sdk` API, `Variable.get` for configuration, SQL kept
next to the DAGs, and ruff for lint and format. Dependencies are managed
with `uv`.

```
dags/
  dvf_postgres_pipeline.py   loader DAG: download -> schema sync -> streaming COPY
  dvf_change_detector.py     daily DAG: detects new/changed yearly files and triggers the loader
  dvf_common.py              shared config (no DAG; listed in .airflowignore)
sql/init.sql                 idempotent DDL: partitioned table, audit log, analytics views
docker/postgres/initdb/      first-boot script: creates the dvf role + database
docker-compose.yaml          Postgres 16 + Airflow (api-server, scheduler/LocalExecutor, dag-processor)
tests/                       unit tests + DAG integrity test (run in CI)
.github/workflows/ci.yml     ruff check, ruff format --check, pytest
```

## 1. Local tooling (uv)

The venv is for your IDE, ruff and pytest. Airflow itself runs in Docker.
Airflow **must** be installed with its constraints file.

```bash
uv venv --python 3.14
source .venv/bin/activate
uv pip install -r pyproject.toml --extra dev \
  -c https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.14.txt

ruff check .            # lint (ruff defaults + isort)
ruff format .           # format
pytest                  # 28 tests, ~3s
pre-commit install      # optional: same checks on every commit
```

## 2. Start the stack

```bash
cp .env.example .env    # optional, all values have defaults
docker compose up -d
```

| What | Where |
|---|---|
| Airflow UI (no login, local dev only) | http://localhost:8080 |
| DVF database | `postgresql://dvf:dvf@localhost:5433/dvf` |
| Downloaded files | `./data/dvf/<year>/full.csv.gz` |
| Postgres data directory | `./pgdata/` |

On first start, Postgres creates two databases: `airflow` (metadata) and `dvf`
(data, owned by a dedicated `dvf` role). It then applies `sql/init.sql` to `dvf`.

## 3. Load data

**All published years (recommended):** trigger the change detector. It loads
every year that is missing or out of date and skips the rest. It also runs
on its own every day at 06:00 UTC.

```bash
docker compose exec airflow-scheduler airflow dags trigger dvf_change_detector
```

**One specific year:**

```bash
docker compose exec airflow-scheduler airflow dags trigger dvf_postgres_pipeline -c '{"year": 2024}'
```

Re-running any year is safe. The year is **replaced** in one transaction, never appended.

## 4. How it works

### Loader (`dvf_postgres_pipeline`)

```
download_dvf_file ─┐
                   ├─> load_dvf_into_postgres
sync_schema ───────┘
```

1. **download_dvf_file**: streams `{DVF_BASE_URL}/{year}/full.csv.gz` to disk in
   1 MiB chunks into a `.part` file, then renames it atomically. It records the
   ETag and size in a `.meta.json` sidecar and skips the download when they
   still match.
2. **sync_schema**: applies `sql/init.sql` (idempotent) and creates the
   `dvf.mutations_<year>` partition. A Postgres advisory lock serializes DDL
   when several years load in parallel.
3. **load_dvf_into_postgres**: reads the gzip as a stream with `csv.reader`
   and cleans each row on the fly:
   - typed converters for int, decimal, float and date
   - `'000001'` → 1 and `'3.0'` → 3
   - leading zeros kept in codes
   - lat/lon range checks
   - INSEE commune/department codes validated, including Corsica `2A`/`2B`

   Rows are written to a `StringIO` buffer and sent with `COPY ... FROM STDIN`
   every `DVF_COPY_CHUNK_ROWS` rows. The same transaction does
   `TRUNCATE partition` → COPY chunks → quality gates → `load_log` insert →
   `ANALYZE` → commit.

**Idempotency.** Any failure rolls back the whole year, which keeps its
previous data. The `(annee, num_ligne_source)` primary key also makes a
duplicate load impossible.

**Quality gates (the load is rolled back if one fails):**
- 0 rows loaded
- reject ratio above `DVF_MAX_REJECT_RATIO`
- the row count in the table differs from the rows streamed
- the source schema has drifted (a missing column fails fast; columns are mapped by name)

**Memory.** Only one chunk (about 15 MB) is in RAM at a time. When the 2024
load was measured (3.5M rows), the worker container used about 140 MiB above
its idle baseline.

**psycopg 2 vs 3.** Under the Airflow 3.3 constraints (SQLAlchemy 2 +
psycopg 3), `PostgresHook.get_conn()` returns a **psycopg 3** connection,
which has no `copy_expert`. `copy_buffer()` supports both drivers.

### Change detection (`dvf_change_detector`)

```
detect_changes ──> trigger_reload  (mapped: one loader run per new/changed year)
```

1. Reads the `/latest/csv/` index to discover the published years. Etalab
   keeps the last 5.
2. Sends one `HEAD` per year to get the ETag and size of the remote file.
   Nothing is downloaded.
3. Compares them with the **last committed load** of that year
   (`dvf.v_load_status`, built from `dvf.load_log`):
   - never loaded → **new**
   - ETag or size differs → **changed**
   - otherwise → skipped
4. Triggers `dvf_postgres_pipeline` for those years only, with run_id
   `dvf_change__<year>__<file-hash>__<YYYYMMDD>` and
   `skip_when_already_exists`. Re-running the detector on the same day never
   queues a duplicate.

The database is the source of truth. A failed or rolled-back load leaves the
old ETag in `load_log`, so the year is still "changed" the next day and is
retried automatically under a new run_id. This self-healing was verified by
simulating a republish (altering a stored ETag).

Years that drop off `/latest` are only logged. Their data stays in Postgres.

To check freshness by hand:

```sql
SELECT annee, source_etag, rows_loaded, loaded_at FROM dvf.v_load_status;
```

## 5. Configuration

**Airflow Variables.** They're set as `AIRFLOW_VAR_*` in `docker-compose.yaml`
and read at **run time**, not at parse time.

| Variable | Default | |
|---|---|---|
| `DVF_BASE_URL` | `https://files.data.gouv.fr/geo-dvf/latest/csv` | Point it at a dated release (e.g. `.../geo-dvf/2025-12/csv`) for reproducible loads |
| `DVF_DATA_DIR` | `/opt/airflow/data/dvf` | Mounted from `./data` |
| `DVF_SQL_DIR` | `/opt/airflow/sql` | Mounted from `./sql` (read-only) |
| `DVF_POSTGRES_CONN_ID` | `dvf_postgres` | |
| `DVF_COPY_CHUNK_ROWS` | `50000` | Rows per COPY / RAM bound |
| `DVF_MAX_REJECT_RATIO` | `0.001` | Fails the load above 0.1% rejected rows |

Variables defined as env vars take precedence over the UI. To manage one in
the UI, delete it from the compose file.

**Connection.** `AIRFLOW_CONN_DVF_POSTGRES` (JSON) defines `dvf_postgres` at
startup. It doesn't appear in the UI list, but
`airflow connections get dvf_postgres` resolves it.

## 6. Querying DVF correctly

One mutation (`id_mutation`) spans **several rows** (one per parcel or
lot), and each row **repeats the total `valeur_fonciere`**. Never
`SUM(valeur_fonciere)` over `dvf.mutations`. Use `dvf.v_mutations`, which
has one row per mutation:

```sql
-- Median price of single-apartment sales, by department, 2024
SELECT code_departement,
       count(*) AS ventes,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY valeur_fonciere) AS prix_median
FROM dvf.v_mutations
WHERE annee = 2024 AND nature_mutation = 'Vente'
  AND nb_appartements = 1 AND nb_maisons = 0
GROUP BY 1 ORDER BY 2 DESC LIMIT 10;
```

## 7. Operations

```bash
docker compose logs -f airflow-scheduler       # task output also lands in ./logs
docker compose down                            # stop; keeps ./pgdata and ./data
docker compose down && rm -rf pgdata data/dvf  # full reset (initdb runs again)
```

- **Linux:** run `echo "AIRFLOW_UID=$(id -u)" >> .env` before the first `up`.
- **macOS/Windows:** `PGDATA` points at a **subdirectory** of the bind mount.
  If it pointed at the mount root, Docker Desktop's file sharing would break
  `initdb` with "data directory has wrong ownership".
- **Airflow version upgrades:** change the image tag, the `apache-airflow` pin,
  and the constraints URL (README + CI) together.
- **Ruff upgrades:** the pin is in `pyproject.toml`, `.pre-commit-config.yaml`
  and `ci.yml`. New ruff versions add default rules, which is why it's pinned.
