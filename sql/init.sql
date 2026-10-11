-- =============================================================================
-- DVF (Demandes de Valeurs Foncières), géolocalisées by Etalab
-- Destination schema for the dvf_postgres_pipeline DAG.
--
-- Source : https://files.data.gouv.fr/geo-dvf/latest/csv/{year}/full.csv.gz
-- Doc    : https://doc.data.gouv.fr/donnees-foncieres/dvf-geolocalisees/
--
-- This script is IDEMPOTENT and safe to run any number of times. Two things run it:
--   * docker/postgres/initdb/00_create_dvf_database.sh (first boot of the cluster)
--   * the DAG's `sync_schema` task (every run)
-- It never drops or truncates data. Per-year cleaning happens in the load
-- task, inside the same transaction as the COPY.
--
-- Typing choices come from profiling the 2021 and 2025 files (~4.7M rows/year):
--   * Codes keep their leading zeros and Corsican letters (01004, 2A004, 2B), so
--     they are VARCHAR, never INTEGER.
--   * lot*_numero / numero_volume contain letters ("11A", "DEUX"), so TEXT.
--   * valeur_fonciere goes up to 1.4e10 (NUMERIC(15,2) holds up to 1e13).
--   * surface_terrain goes up to 2.5e8, which fits INTEGER. Carrez surfaces have decimals.
--
-- ANALYTICS PITFALL: in DVF, one mutation (id_mutation) spans several rows
-- (one per parcel or lot), and every row repeats the TOTAL valeur_fonciere of
-- the mutation. Never SUM(valeur_fonciere) over raw rows. Use the
-- dvf.v_mutations view below, which has one row per mutation.
-- =============================================================================

-- PostGIS is not a trusted extension: a superuser creates it (initdb does it
-- on first boot). Fail with a clear message instead of "type geometry does not exist".
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_extension WHERE extname = 'postgis') THEN
        RAISE EXCEPTION 'PostGIS is missing in this database. As a superuser run: '
                        'CREATE EXTENSION postgis;';
    END IF;
END
$$;

CREATE SCHEMA IF NOT EXISTS dvf;

COMMENT ON SCHEMA dvf IS
    'DVF géolocalisées (Etalab) loaded by the Airflow DAG dvf_postgres_pipeline';

-- -----------------------------------------------------------------------------
-- Main table, LIST-partitioned by source year (one partition per yearly file).
-- Partitions are created by the DAG (dvf.mutations_<year>). Reloading a year is
-- a TRUNCATE of its partition plus a COPY, in a single transaction.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dvf.mutations (
    -- Technical tracking keys --------------------------------------------------
    annee                         SMALLINT      NOT NULL,  -- year of the source file (partition key)
    num_ligne_source              INTEGER       NOT NULL,  -- 1-based data line number in full.csv.gz

    -- Mutation -----------------------------------------------------------------
    id_mutation                   VARCHAR(16)   NOT NULL,  -- e.g. '2024-123456'; NOT unique (1 row per parcel/lot)
    date_mutation                 DATE          NOT NULL,
    numero_disposition            INTEGER,
    nature_mutation               TEXT,                    -- Vente, VEFA, Echange, Adjudication...
    valeur_fonciere               NUMERIC(15,2),           -- EUR, total for the mutation (repeated on each row)

    -- Address ------------------------------------------------------------------
    adresse_numero                INTEGER,
    adresse_suffixe               VARCHAR(4),              -- B, T, Q...
    adresse_nom_voie              TEXT,
    adresse_code_voie             VARCHAR(8),              -- FANTOIR/RIVOLI code (alphanumeric)
    code_postal                   VARCHAR(5),

    -- Administrative keys (INSEE) ------------------------------------------------
    code_commune                  VARCHAR(5)    NOT NULL,  -- INSEE code: '75056', '2A004'
    nom_commune                   TEXT,
    code_departement              VARCHAR(3)    NOT NULL,  -- '01', '2A', '974'
    ancien_code_commune           VARCHAR(5),
    ancien_nom_commune            TEXT,

    -- Cadastre -----------------------------------------------------------------
    id_parcelle                   VARCHAR(14),             -- 14-char cadastral id
    ancien_id_parcelle            VARCHAR(14),
    numero_volume                 TEXT,

    -- Co-ownership lots (first 5) -----------------------------------------------
    lot1_numero                   TEXT,
    lot1_surface_carrez           NUMERIC(10,2),           -- m², 2 decimals
    lot2_numero                   TEXT,
    lot2_surface_carrez           NUMERIC(10,2),
    lot3_numero                   TEXT,
    lot3_surface_carrez           NUMERIC(10,2),
    lot4_numero                   TEXT,
    lot4_surface_carrez           NUMERIC(10,2),
    lot5_numero                   TEXT,
    lot5_surface_carrez           NUMERIC(10,2),
    nombre_lots                   INTEGER,

    -- Premises -----------------------------------------------------------------
    code_type_local               SMALLINT,                -- 1 Maison, 2 Appartement, 3 Dépendance, 4 Local
    type_local                    TEXT,
    surface_reelle_bati           INTEGER,                 -- m²
    nombre_pieces_principales     SMALLINT,

    -- Land ---------------------------------------------------------------------
    code_nature_culture           VARCHAR(4),
    nature_culture                TEXT,
    code_nature_culture_speciale  VARCHAR(8),
    nature_culture_speciale       TEXT,
    surface_terrain               INTEGER,                 -- m²

    -- Geolocation (WGS84) --------------------------------------------------------
    longitude                     DOUBLE PRECISION,
    latitude                      DOUBLE PRECISION,

    -- Audit --------------------------------------------------------------------
    loaded_at                     TIMESTAMPTZ   NOT NULL DEFAULT now(),

    -- (annee, num_ligne_source) identifies a source row exactly. A duplicate
    -- load of the same file is therefore impossible, even by mistake.
    CONSTRAINT mutations_pkey PRIMARY KEY (annee, num_ligne_source),
    CONSTRAINT mutations_valeur_fonciere_chk CHECK (valeur_fonciere IS NULL OR valeur_fonciere >= 0),
    CONSTRAINT mutations_latitude_chk        CHECK (latitude  IS NULL OR latitude  BETWEEN -90  AND 90),
    CONSTRAINT mutations_longitude_chk       CHECK (longitude IS NULL OR longitude BETWEEN -180 AND 180)
) PARTITION BY LIST (annee);

COMMENT ON TABLE dvf.mutations IS
    'DVF géolocalisées, one row per (mutation x parcel/lot); partitioned by source year';
COMMENT ON COLUMN dvf.mutations.valeur_fonciere IS
    'Total price of the MUTATION, repeated on each of its rows. Do not SUM over raw rows.';

-- Indexes declared on the parent are created on every partition, including
-- partitions created later.
CREATE INDEX IF NOT EXISTS mutations_id_mutation_idx       ON dvf.mutations (id_mutation);
CREATE INDEX IF NOT EXISTS mutations_commune_date_idx      ON dvf.mutations (code_commune, date_mutation);
CREATE INDEX IF NOT EXISTS mutations_departement_date_idx  ON dvf.mutations (code_departement, date_mutation);
CREATE INDEX IF NOT EXISTS mutations_id_parcelle_idx       ON dvf.mutations (id_parcelle);

-- PostGIS point derived from longitude/latitude (WGS84, valid for overseas
-- departments too). STORED generated column: COPY never writes it, Postgres
-- computes it, and it can't drift from the coordinates. NULL when either
-- coordinate is missing. Checked first so that a routine run doesn't take
-- an ACCESS EXCLUSIVE lock on the whole table.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT FROM information_schema.columns
        WHERE table_schema = 'dvf' AND table_name = 'mutations' AND column_name = 'geom'
    ) THEN
        ALTER TABLE dvf.mutations ADD COLUMN geom geometry(Point, 4326)
            GENERATED ALWAYS AS (ST_Point(longitude, latitude, 4326)) STORED;
    END IF;
END
$$;
CREATE INDEX IF NOT EXISTS mutations_geom_idx ON dvf.mutations USING gist (geom);

-- -----------------------------------------------------------------------------
-- Official list of communes (all of France), replaced as a whole by the DAG
-- task load_communes_reference. It gives every INSEE code ONE canonical name
-- (DVF rows can carry an old name, e.g. Cadillac -> Cadillac-sur-Garonne),
-- plus EPCI, population and area. The semantic layer reads it through
-- dvf.communes.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dvf.ref_communes (
    code_insee       VARCHAR(5)    PRIMARY KEY,
    nom              TEXT          NOT NULL,
    nom_sans_accent  TEXT,
    dep_code         VARCHAR(3)    NOT NULL,
    epci_code        VARCHAR(9),
    epci_nom         TEXT,
    code_postal      VARCHAR(5),               -- main postal code only
    population       INTEGER,
    superficie_km2   NUMERIC(10,2),
    densite          NUMERIC(10,1),            -- inhabitants per km²
    latitude         DOUBLE PRECISION,         -- centre of the commune (WGS84)
    longitude        DOUBLE PRECISION,
    grille_densite   TEXT,                     -- INSEE density grid label
    millesime        SMALLINT                  -- edition of the list (e.g. 2026)
);

COMMENT ON TABLE dvf.ref_communes IS
    'Official list of communes of France (data.gouv.fr "Liste des communes de France", '
    'Licence Ouverte 2.0), loaded from DVF_COMMUNES_URL';

-- -----------------------------------------------------------------------------
-- Rent indicators "Carte des loyers" (ANIL / ministry of housing), one row per
-- (edition year, segment, commune), all of France. Loaded by the DAG
-- dvf_loyers_pipeline for the DVF years (no edition exists for 2021).
-- loyer_m2 is a PREDICTED advertised rent, EUR/m², charges included, for an
-- unfurnished reference property advertised in Q3 of `annee`: not a median nor
-- a mean, and editions are NOT comparable over time (mailles change yearly).
-- Credit required: "Estimations ANIL, à partir des données du Groupe SeLoger
-- et de leboncoin".
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dvf.ref_loyers (
    annee              SMALLINT      NOT NULL,   -- edition = ads of Q3 of that year
    segment            TEXT          NOT NULL,   -- appartement | appartement_1_2_pieces | appartement_3_pieces_plus | maison
    code_insee         VARCHAR(5)    NOT NULL,
    nom_commune        TEXT,
    dep_code           VARCHAR(3)    NOT NULL,
    epci_code          VARCHAR(9),
    id_maille          TEXT,                     -- group of communes used for the estimate
    loyer_m2           NUMERIC(8,2)  NOT NULL,   -- EUR/m², charges included
    loyer_m2_bas       NUMERIC(8,2),             -- 95% prediction interval
    loyer_m2_haut      NUMERIC(8,2),
    niveau_prediction  TEXT,                     -- commune | epci | maille
    nb_obs_commune     INTEGER,                  -- ads seen in the commune (< 30 = unreliable)
    nb_obs_maille      INTEGER,
    r2_ajuste          NUMERIC(6,4),             -- < 0.5 = unreliable
    CONSTRAINT ref_loyers_pkey PRIMARY KEY (annee, segment, code_insee),
    CONSTRAINT ref_loyers_segment_chk CHECK (segment IN (
        'appartement', 'appartement_1_2_pieces', 'appartement_3_pieces_plus', 'maison'))
);

COMMENT ON TABLE dvf.ref_loyers IS
    'Carte des loyers (ANIL): predicted advertised rent EUR/m² per commune, Q3 of annee. '
    'Not comparable across years. Credit: Estimations ANIL, à partir des données du '
    'Groupe SeLoger et de leboncoin';

-- -----------------------------------------------------------------------------
-- Load audit trail: one row per successful (committed) load of a year.
-- Written in the SAME transaction as the data, so it never lies.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dvf.load_log (
    id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    annee               SMALLINT     NOT NULL,
    source_url          TEXT         NOT NULL,
    source_etag         TEXT,
    source_size_bytes   BIGINT,
    rows_read           INTEGER      NOT NULL,
    rows_loaded         INTEGER      NOT NULL,
    rows_rejected       INTEGER      NOT NULL,
    values_nulled       INTEGER      NOT NULL,   -- unparseable optional values set to NULL
    dag_run_id          TEXT,
    duration_seconds    NUMERIC(10,1),
    loaded_at           TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- Added later: the source file's Last-Modified (identifies the DVF release).
ALTER TABLE dvf.load_log ADD COLUMN IF NOT EXISTS source_last_modified TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS load_log_annee_loaded_at_idx ON dvf.load_log (annee, loaded_at DESC);

-- Latest committed load per year. The dvf_change_detector DAG compares
-- source_etag/source_size_bytes with what data.gouv.fr serves now to decide
-- which years to reload.
CREATE OR REPLACE VIEW dvf.v_load_status AS
SELECT DISTINCT ON (annee)
    annee,
    source_url,
    source_etag,
    source_size_bytes,
    rows_loaded,
    rows_rejected,
    dag_run_id,
    loaded_at,
    source_last_modified
FROM dvf.load_log
ORDER BY annee, loaded_at DESC;

-- -----------------------------------------------------------------------------
-- Analytics view: ONE row per mutation, with deduplicated price and
-- aggregated surfaces. This is the right grain for price-per-m² studies.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW dvf.v_mutations AS
SELECT
    m.annee,
    m.id_mutation,
    min(m.date_mutation)                                   AS date_mutation,
    min(m.nature_mutation)                                 AS nature_mutation,
    max(m.valeur_fonciere)                                 AS valeur_fonciere,
    min(m.code_commune)                                    AS code_commune,
    min(m.code_departement)                                AS code_departement,
    count(*)                                               AS nb_lignes,
    count(DISTINCT m.id_parcelle)                          AS nb_parcelles,
    count(*) FILTER (WHERE m.code_type_local = 1)          AS nb_maisons,
    count(*) FILTER (WHERE m.code_type_local = 2)          AS nb_appartements,
    count(*) FILTER (WHERE m.code_type_local = 3)          AS nb_dependances,
    count(*) FILTER (WHERE m.code_type_local = 4)          AS nb_locaux,
    sum(m.surface_reelle_bati)                             AS surface_reelle_bati_totale,
    sum(m.surface_terrain)                                 AS surface_terrain_totale,
    avg(m.longitude)                                       AS longitude,
    avg(m.latitude)                                        AS latitude
FROM dvf.mutations AS m
GROUP BY m.annee, m.id_mutation;

COMMENT ON VIEW dvf.v_mutations IS
    'One row per mutation. Caveat: DVF emits one row per (local x culture), so surfaces can be '
    'double-counted on multi-row mutations; for price/m2, prefer single-local sales (e.g. nb_maisons + nb_appartements = 1).';
