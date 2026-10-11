# dvf-airflow-pipeline

Loads **DVF géolocalisées** (Demandes de Valeurs Foncières, published by Etalab on
data.gouv.fr) into **PostgreSQL 16 + PostGIS** with **Apache Airflow 3.3.2** on
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
  dvf_loyers_pipeline.py     monthly DAG: rent indicators "Carte des loyers" for the DVF years
  dvf_logement_pipeline.py   monthly DAG: social housing (RPLS), vacancy (LOVAC), long series, CDC costs
  dvf_accessibilite_pipeline.py  monthly DAG: walking graph (OSM + pgRouting), schools, transport, addresses
  dvf_osm.py / dvf_transit.py    OSM -> walking graph; GTFS -> transit times (no DAG; .airflowignore)
  dvf_common.py              shared config (no DAG; listed in .airflowignore)
  dvf_io.py                  shared converters, COPY and download helpers (no DAG; .airflowignore)
sql/init.sql                 idempotent DDL: partitioned table (+ PostGIS geom), audit log, reference tables
sql/semantic.sql             semantic layer rebuilt after each load: dvf.ventes, dvf.biens, dvf.communes, dvf.lieux, dvf.zones
docker/postgres/Dockerfile   postgres:16 + PostGIS 3 + pgRouting (arm64 and amd64)
docker/airflow/Dockerfile    apache/airflow:3.3.2-python3.14 + xlrd (legacy .xls) + osmium (OpenStreetMap)
docker/postgres/initdb/      first-boot script: creates the dvf role + database, enables PostGIS
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
pytest                  # 38 tests, ~20s
pre-commit install      # optional: same checks on every commit
```

## 2. Start the stack

```bash
cp .env.example .env    # optional, all values have defaults
docker compose up -d    # builds the Postgres + PostGIS image on first run
```

| What | Where |
|---|---|
| Airflow UI (no login, local dev only) | http://localhost:8080 |
| DVF database | `postgresql://dvf:dvf@localhost:5433/dvf` |
| Downloaded files | `./data/dvf/<year>/full.csv.gz` |
| Postgres data directory | `./pgdata/` |

On first start, Postgres creates two databases: `airflow` (metadata) and `dvf`
(data, owned by a dedicated `dvf` role). It enables PostGIS in `dvf` (PostGIS
is not a trusted extension, so the superuser does it), then applies
`sql/init.sql`.

**Upgrading an existing `./pgdata`** created before PostGIS was added: the
image keeps the same Debian base and Postgres major, so the data directory is
reused as is. Enable the extension once, then reload any year (or run the two
SQL files) to add the geometry column and build the semantic layer:

```bash
docker compose up -d --build
docker compose exec postgres psql -U airflow -d dvf -c 'CREATE EXTENSION IF NOT EXISTS postgis'
```

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

Re-running any year is safe. That year's partition is **replaced** in one
transaction; every other year is left untouched (see *History* below).

**Rents (`dvf.ref_loyers`):** loaded by `dvf_loyers_pipeline` for the years
DVF covers. It also runs on the 2nd of every month and only replaces what
changed.

```bash
docker compose exec airflow-scheduler airflow dags trigger dvf_loyers_pipeline
```

## 4. How it works

### Loader (`dvf_postgres_pipeline`)

```
download_dvf_file ─┐
                   ├─> load_dvf_into_postgres ───┐
sync_schema ───────┼─────────────────────────────┼─> refresh_semantic_layer
                   └─> load_communes_reference ──┘
```

1. **download_dvf_file**: streams `{DVF_BASE_URL}/{year}/full.csv.gz` to disk in
   1 MiB chunks into a `.part` file, then renames it atomically. It records the
   ETag, size and Last-Modified in a `.meta.json` sidecar and skips the
   download when they still match.
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
4. **load_communes_reference**: downloads the official list of communes
   (`DVF_COMMUNES_URL`, data.gouv.fr "Liste des communes de France",
   Licence Ouverte 2.0) and replaces `dvf.ref_communes` (~34 900 communes:
   INSEE code, current name, EPCI, population, area, centre) in one
   transaction. It refuses a list with fewer than 30 000 communes.
5. **refresh_semantic_layer**: applies `sql/semantic.sql` under the same
   advisory lock (about 15 s for Gironde). See section 6.

**Idempotency.** Any failure rolls back the whole year, which keeps its
previous data. The `(annee, num_ligne_source)` primary key also makes a
duplicate load impossible.

**Quality gates (the load is rolled back if one fails):**
- 0 rows loaded
- reject ratio above `DVF_MAX_REJECT_RATIO`
- the row count in the table differs from the rows streamed
- the source schema has drifted (a missing column fails fast; columns are mapped by name)
- **format drift**: an optional column whose unparseable values (set to NULL)
  exceed `DVF_MAX_REJECT_RATIO`, e.g. prices with decimal commas
- **history**: the year would shrink by more than `DVF_MAX_SHRINK_RATIO`
  compared with its previous committed load

**Format.** Etalab's files are comma-separated with decimal points, unlike the
raw DGFiP export (`|` separators, decimal commas, French headers). Feeding a
DGFiP file fails the schema check; a decimal comma inside an Etalab file fails
the format gate instead of being silently NULLed. Prices are `NUMERIC(15,2)`
parsed with `Decimal`, so cents are exact. Cross-check: the same method as
Etalab's `statistiques-dvf` run on the loaded data reproduces their published
Bordeaux monthly medians (apartments and houses, counts and EUR/m²) exactly for
23 of 24 months of 2024-2025.

**History (append by year).** DVF covers ~5 years and each release (April
and October) drops the oldest one. A truncate-and-reload of the whole table
would lose that year. Here a load only ever replaces **its own year's
partition**: years that leave `/latest` are never touched again and stay in
Postgres, and a republished year that comes back much smaller is refused
(shrink gate). `load_log.source_last_modified` records which release each year
came from. Neither data.gouv.fr nor Etalab serves years before 2021 any more,
so history can only accumulate from the first load onward: don't delete
`./pgdata` lightly.

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
SELECT annee, source_last_modified, rows_loaded, loaded_at FROM dvf.v_load_status;
```

### Rents (`dvf_loyers_pipeline`)

```
sync_schema ──> plan_editions ──> load_edition  (mapped: one per year)
```

Source: "Carte des loyers", indicators of advertised rents per commune
(ANIL / ministry of housing, data.gouv.fr, Licence Ouverte 2.0). One dataset
per edition, four files each: apartments, apartments with 1-2 rooms, 3+
rooms, houses.

1. **plan_editions** takes the DVF years already loaded and asks the
   data.gouv.fr API for each year's dataset. There is **no 2021 edition** (the
   series goes 2018, then 2022 onwards), so 2021 is logged and skipped:
   rents cover 2022-2025.
2. **load_edition** downloads the four files (ETag skip) and replaces that
   year of `dvf.ref_loyers` in one transaction, with the same gates as DVF:
   at least 30 000 communes per file, reject ratio and format drift.

**Format trap.** These files are `;`-separated, with decimal **commas**, in
Windows-1252, and static.data.gouv.fr serves them gzip-compressed
(`Content-Encoding: gzip`). The loader handles all four.

**What a value means.** `loyer_m2` is a *predicted* advertised rent in
EUR/m², charges included, for an unfurnished reference property (apartment
52 m², house 92 m²) advertised in Q3 of `annee`. It is neither a median nor a
mean. **Editions are not comparable over time**: the groups of communes used
for the estimate change every year. Treat a value with caution when
`nb_obs_commune < 30` or `r2_ajuste < 0.5`; in Gironde that is about 70% of
the communes, so only the larger ones are reliable. Any reuse must credit
"Estimations ANIL, à partir des données du Groupe SeLoger et de leboncoin".

Rents are not in the semantic layer yet: the text-to-SQL engine can't query
them.

### Housing reference data (`dvf_logement_pipeline`)

```
sync_schema ──┬─> plan_rpls ──> load_rpls (mapped: one per edition)
              ├─> load_lovac
              ├─> load_series_longues
              └─> load_cdc
```

Each source replaces its own table (RPLS: its own edition) in one
transaction, with the usual gates. Monthly; every source is annual.

| Table | Source | Coverage | Traps |
|---|---|---|---|
| `dvf.ref_rpls` | RPLS, social housing stock (SDES, DiDo API), one row per dwelling | Gironde (`DVF_RPLS_DEPARTEMENTS`), 1 January 2022-2025 (none before 2022); ~117 000 dwellings | columns renamed between editions (`DEPCOM` / `DEPCOM_CODE`, IRIS 2022 / 2024): mapped by aliases; coordinates already Lambert-93, `geom` is generated |
| `dvf.ref_lovac` | LOVAC, vacant private dwellings (DGFiP / Cerema) | commune, department, region, France; editions 2020-2026 | wide files unpivoted; `"s"` = statistical secret, stored NULL + `*_secret` flag (commune values don't add up: use the department level); thousands separators; a region column renamed (`longue_duree`); edition N ≈ 1 January N-1; **method break in 2025** (Gironde: 83 000 → 50 000 vacant); no private stock for 2026 |
| `dvf.ref_series_longues` | IGEDD long series 1800-2020 (J. Friggit), legacy `.xls` | France, United States, United Kingdom; 45 series | amounts in EUR even before 1999 (1 EUR = 6.55957 FRF); indices on base 1 in 2000; recent values may be provisional (the red marking can't be read reliably) |
| `dvf.ref_cdc_logement_social` | Caisse des Dépôts, median surfaces and cost prices of financed social housing | by **region** and signing year, 2018-2025 | Gironde only through Nouvelle-Aquitaine (75); renovation values missing for the last year |

Formats: RPLS and CDC are `;` with decimal points; LOVAC is `;`,
Windows-1252, with thousands separators. static.data.gouv.fr and the CDC API
send plain `.csv` files gzip-compressed (`Content-Encoding: gzip`):
`dvf_io.read_downloaded` gunzips by magic number, whatever the file name.

### Accessibility for a home search (`dvf_accessibilite_pipeline`)

```
sync_schema ──> load_contours ──┬─> load_walking_graph ──┐
                                ├─> load_schools ────────┤
                                ├─> load_addresses ──────┼─> compute_access
                                └─> load_transport ──────┘
```

Scope: `DVF_SCOPE_DEPARTEMENTS` (default `33`). Every source is national;
the walking graph is what limits the scale on a laptop.

- **Walking graph** (`dvf.osm_pieton`, `dvf.osm_pieton_noeuds`): OpenStreetMap
  (Geofabrik extract), ways a pedestrian may use (no motorways, no
  `foot=no` / private), split at every shared node, so OSM node ids are the
  pgRouting vertices. Cost: seconds at 4.5 km/h, steps count double. Gironde:
  ~430 000 segments, ~325 000 vertices; vertices of components smaller than
  1 000 vertices are not used for snapping.
- **Schools** (`dvf.ref_ecoles`, `dvf.ref_ecoles_ips`, `dvf.ref_colleges_ivac`):
  annuaire de l'éducation, social position index (IPS: a social profile, not a
  quality score; "NS" = too small to publish -> NULL), collège value added.
  SEGPA sections, sections attached to a lycée and schools without pupils are
  kept but never count as "the nearest school"; "lycée" means general,
  technological or multi-track.
- **Addresses** (`dvf.ref_adresses`): Base Adresse Nationale, with their
  cadastral parcels (same ids as DVF `id_parcelle`).
- **Public transport** (`dvf.transport_arrets`, `dvf.transport_temps_pole`):
  GTFS of TBM, TransGironde and SNCF (`DVF_GTFS_FEEDS`). pgRouting can't follow
  timetables, so a backward connection scan (`dvf_transit.py`) computes, for
  every stop, the time to each pole of `dvf.acces_poles` (default: Centre de
  Bordeaux, Grand Théâtre) for arrivals between 07:30 and 09:00 on a reference
  Tuesday: best case and **typical** (median over 46 deadlines). The wait
  before the first departure is not counted.
- **compute_access**, in pgRouting: snap schools, stops and addresses to the
  nearest vertex; one multi-source `pgr_drivingDistance(..., equicost)` per
  school level (capped at 30 min); one search from a virtual vertex linked to
  every stop (cost = its typical transit time) and to the pole (walking only),
  capped at 90 min, gives door-to-centre time for every vertex.

`dvf.v_acces_adresses` gives, per address, the walking minutes to the nearest
maternelle, élémentaire, collège and lycée, and the typical door-to-centre
minutes. In Gironde, 60% of addresses have an elementary school within 15 min
on foot, and 8.6% are within 30 min of the centre of Bordeaux.

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
| `DVF_MAX_REJECT_RATIO` | `0.001` | Fails the load above 0.1% rejected rows, or 0.1% unparseable values in a column |
| `DVF_MAX_SHRINK_RATIO` | `0.2` | Refuses to replace a year with a version more than 20% smaller |
| `DVF_COMMUNES_URL` | `.../communes-france-2026.csv.gz` | Official list of communes; point it at a newer edition when one is published |
| `DVF_RPLS_DEPARTEMENTS` | `33` | Departments of the RPLS download (comma-separated); the national file is ~5.4 M dwellings per edition |
| `DVF_LOVAC_DATASET` | `logements-vacants-...-de-2020-a-2026` | data.gouv.fr slug of LOVAC; it carries the last edition, update it when a new one is published |
| `DVF_SCOPE_DEPARTEMENTS` | `33` | Departments of the accessibility data (outlines, graph, schools, addresses, stops) |
| `DVF_OSM_PBF_URL` | Geofabrik `aquitaine-latest.osm.pbf` | OpenStreetMap extract containing the scope |
| `DVF_GTFS_FEEDS` | TBM, TransGironde, SNCF | JSON `{"network": "gtfs zip url"}` |
| `DVF_SERIES_LONGUES_URL` | `.../valeur-immobilier-1800-2020_cle2abd1f.xls` | IGEDD long series workbook |

Variables defined as env vars take precedence over the UI. To manage one in
the UI, delete it from the compose file.

**Connection.** `AIRFLOW_CONN_DVF_POSTGRES` (JSON) defines `dvf_postgres` at
startup. It doesn't appear in the UI list, but
`airflow connections get dvf_postgres` resolves it.

## 6. Querying DVF correctly (semantic layer)

Query `sql/semantic.sql`'s objects, not `dvf.mutations`. They are scoped to
Gironde (change the `SCOPE` filter to widen them) and rebuilt after each load.

| Object | Grain | Use for |
|---|---|---|
| `dvf.ventes` | 1 row per mutation | counts of sales, totals, prices, `prix_m2` |
| `dvf.biens` | 1 row per deduplicated property | "N appartements vendus", surfaces, pièces |
| `dvf.lieux` | landmark | distances (Place de la Bourse, Gare Saint-Jean...) |
| `dvf.zones` | (zone, commune) | Bordeaux Métropole, Bassin d'Arcachon |
| `dvf.communes` | commune | one canonical name per INSEE code (official list), postal codes seen in DVF, EPCI, population |

Traps the layer handles (all found in the data):

- **Duplication.** One mutation spans several rows and each row repeats the
  total `valeur_fonciere`: in Gironde 2024 a naive `SUM` over rows gives
  73.5 bn EUR instead of 8.84 bn. DVF also repeats a local once per nature de
  culture of its parcel (14 371 raw `Maison` rows for 12 388 houses).
  `dvf.biens` keeps the max count across cultures, so identical flats sold
  together still count separately.
- **Single-property sales.** `vente_reference` = exactly one house or
  apartment (dépendances allowed), nature Vente or VEFA, price > 0. `prix_m2`
  is only set on those rows.
- **VEFA.** ~95% of VEFA sales have no `type_local` and no surface (lots of a
  building not yet in the cadastre): `type_bien = 'Lot non décrit'`.
- **Missing values** (Gironde 2024): 272 sales without coordinates (they drop
  out of spatial queries), 556 rows without price; surfaces and room counts
  are missing on <0.01% of house/apartment rows.
- **Commune names.** DVF rows can carry an old name (Cadillac, now
  Cadillac-sur-Garonne); `dvf.ventes`, `dvf.biens` and `dvf.communes` all use
  the official current name, and a sale's code and name always come from the
  same row. Postal codes come from DVF, not from the official list, whose
  CEDEX codes (33001, 33012...) look like INSEE codes.
- **Spatial.** `dvf.mutations.geom` is a WGS84 point (all of France, GiST
  indexed, generated from longitude/latitude). The semantic layer's `geom`
  columns are Lambert-93 (EPSG:2154): units are metres, so
  `ST_DWithin(v.geom, l.geom, 500)` means 500 m and uses the GiST index.

```sql
-- Median EUR/m² of apartments within 1 km of Gare Saint-Jean, 2024
SELECT round(percentile_cont(0.5) WITHIN GROUP (ORDER BY v.prix_m2)) AS prix_m2_median
FROM dvf.ventes v JOIN dvf.lieux l ON l.nom = 'Gare Saint-Jean'
WHERE v.annee = 2024 AND v.type_bien = 'Appartement' AND v.prix_m2 IS NOT NULL
  AND ST_DWithin(v.geom, l.geom, 1000);
```

The full set of conventions, with verified gold queries, lives in the text-to-SQL
evaluation set (`dvf_eval_questions.yaml`).

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
