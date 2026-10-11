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

-- Fuzzy commune search (dvf.chercher_commune): trigram similarity and accent
-- folding. Both are trusted extensions: the database owner creates them.
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS unaccent;

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
-- Housing reference data, loaded by the DAG dvf_logement_pipeline. Each table
-- (or RPLS edition) is replaced as a whole. Not part of the semantic layer.
-- -----------------------------------------------------------------------------

-- RPLS: one row per social dwelling at 1 January of `millesime` (SDES).
-- Scope: DVF_RPLS_DEPARTEMENTS (default Gironde). No dwelling identifier is
-- published: num_ligne is the position in the downloaded file.
CREATE TABLE IF NOT EXISTS dvf.ref_rpls (
    millesime               SMALLINT      NOT NULL,   -- situation at 1 January
    num_ligne               INTEGER       NOT NULL,
    code_commune            VARCHAR(5)    NOT NULL,
    nom_commune             TEXT,
    code_postal             VARCHAR(5),
    dep_code                VARCHAR(3),
    epci_code               VARCHAR(9),
    code_iris               VARCHAR(9),               -- IRIS 2024 (2022 for older editions)
    qpv                     TEXT,                     -- in a priority neighbourhood (QPV)?
    type_construction       TEXT,                     -- individuel | collectif
    nb_pieces               SMALLINT,
    surface_habitable       NUMERIC(8,2),             -- m²
    annee_construction      SMALLINT,
    annee_mise_en_location  SMALLINT,
    financement             TEXT,                     -- initial financing (PLAI, PLUS, PLS...)
    droit                   TEXT,
    dpe_energie             TEXT,
    dpe_ges                 TEXT,
    epsg                    TEXT,
    x                       NUMERIC(12,2),
    y                       NUMERIC(12,2),
    geom                    geometry(Point, 2154)
        GENERATED ALWAYS AS (CASE WHEN epsg = '2154'
                                  THEN ST_SetSRID(ST_MakePoint(x::float8, y::float8), 2154) END) STORED,
    CONSTRAINT ref_rpls_pkey PRIMARY KEY (millesime, num_ligne)
);
COMMENT ON TABLE dvf.ref_rpls IS
    'RPLS social housing stock (SDES, Licence Ouverte), one row per dwelling at 1 January of millesime';
CREATE INDEX IF NOT EXISTS ref_rpls_commune_idx ON dvf.ref_rpls (code_commune, millesime);
CREATE INDEX IF NOT EXISTS ref_rpls_geom_idx    ON dvf.ref_rpls USING gist (geom);

-- LOVAC: vacant private dwellings, per edition and geographic level.
-- NULL + *_secret = statistical secret ('s'): never a zero. Edition N mostly
-- describes 1 January N-1; method break in 2025; no private stock for 2026.
CREATE TABLE IF NOT EXISTS dvf.ref_lovac (
    millesime                 SMALLINT  NOT NULL,
    niveau                    TEXT      NOT NULL,   -- commune | departement | region | france
    code                      TEXT      NOT NULL,   -- INSEE code of the level ('FR' for France)
    libelle                   TEXT,
    parc_prive                INTEGER,              -- private dwellings
    vacants                   INTEGER,
    vacants_secret            BOOLEAN   NOT NULL DEFAULT false,
    vacants_plus_2ans         INTEGER,              -- vacant for more than two years
    vacants_plus_2ans_secret  BOOLEAN   NOT NULL DEFAULT false,
    CONSTRAINT ref_lovac_pkey PRIMARY KEY (millesime, niveau, code),
    CONSTRAINT ref_lovac_niveau_chk CHECK (niveau IN ('commune', 'departement', 'region', 'france'))
);
COMMENT ON TABLE dvf.ref_lovac IS
    'LOVAC vacant private dwellings (DGFiP / Cerema, Licence Ouverte), by edition and level';

-- Long series 1800-2020 (IGEDD): one row per (country, series, year).
CREATE TABLE IF NOT EXISTS dvf.ref_series_longues (
    pays       TEXT              NOT NULL,   -- France | USA | United Kingdom (as in the source)
    categorie  TEXT,
    serie      TEXT              NOT NULL,   -- French label, with its unit
    serie_en   TEXT,
    source     TEXT,
    annee      SMALLINT          NOT NULL,
    valeur     DOUBLE PRECISION  NOT NULL,
    CONSTRAINT ref_series_longues_pkey PRIMARY KEY (pays, serie, annee)
);
COMMENT ON TABLE dvf.ref_series_longues IS
    'Long series 1800-2020 (IGEDD): amounts in EUR even before 1999 (1 EUR = 6.55957 FRF); '
    'the latest years may be provisional';

-- DPE (ADEME): energy performance certificates of existing dwellings, since
-- July 2021, scope DVF_SCOPE_DEPARTEMENTS. ADEME publishes only current
-- certificates (the ones they replace are absent), so `remplace` stays false.
-- TRAP: ~21% of apartment certificates are "généré à partir des données DPE
-- immeuble" (methode): one building rating copied to every apartment; count
-- individual ratings with methode = 'dpe appartement individuel'.
-- Rental bans: G since 2025, F from 2028, E from 2034. cout_annuel_eur is the
-- conventional estimate of the DPE, not a bill.
CREATE TABLE IF NOT EXISTS dvf.ref_dpe (
    numero_dpe            TEXT PRIMARY KEY,
    date_dpe              DATE,
    numero_dpe_remplace   TEXT,
    remplace              BOOLEAN NOT NULL DEFAULT false,
    etiquette_dpe         CHAR(1) NOT NULL,
    etiquette_ges         CHAR(1),
    type_batiment         TEXT,           -- maison | appartement | immeuble
    periode_construction  TEXT,
    annee_construction    SMALLINT,
    typologie             TEXT,           -- T1, T2...
    surface_m2            NUMERIC(8,1),
    etage                 SMALLINT,
    nb_niveaux_immeuble   SMALLINT,
    energie_chauffage     TEXT,
    cout_annuel_eur       NUMERIC(10,1),  -- estimated annual energy cost
    conso_ep_m2           NUMERIC(8,1),   -- primary energy, kWh/m²/year
    code_commune          VARCHAR(5),
    adresse               TEXT,
    identifiant_ban       TEXT,
    score_ban             NUMERIC(5,3),   -- geocoding confidence (0-1)
    x                     NUMERIC(12,2),
    y                     NUMERIC(12,2),
    geom                  geometry(Point, 2154)
        GENERATED ALWAYS AS (ST_SetSRID(ST_MakePoint(x::float8, y::float8), 2154)) STORED
);
ALTER TABLE dvf.ref_dpe ADD COLUMN IF NOT EXISTS methode TEXT;  -- individual or generated from the building
CREATE INDEX IF NOT EXISTS ref_dpe_geom_idx ON dvf.ref_dpe USING gist (geom);
CREATE INDEX IF NOT EXISTS ref_dpe_commune_idx ON dvf.ref_dpe (code_commune);
COMMENT ON TABLE dvf.ref_dpe IS
    'ADEME DPE of existing dwellings (Licence Ouverte), one row per certificate';

-- CDC: median surfaces and cost prices of financed social housing operations.
CREATE TABLE IF NOT EXISTS dvf.ref_cdc_logement_social (
    code_region                   VARCHAR(3)    NOT NULL,
    region                        TEXT,
    annee                         SMALLINT      NOT NULL,   -- signing year of the financing
    construction_surface_m2       NUMERIC(8,1),             -- median useful surface per dwelling
    construction_cout_m2          NUMERIC(10,1),            -- median cost price, EUR/m²
    construction_cout_logement    NUMERIC(12,1),            -- median cost price, EUR per dwelling
    rehabilitation_surface_m2     NUMERIC(8,1),
    rehabilitation_cout_m2        NUMERIC(10,1),
    rehabilitation_cout_logement  NUMERIC(12,1),
    CONSTRAINT ref_cdc_logement_social_pkey PRIMARY KEY (code_region, annee)
);
COMMENT ON TABLE dvf.ref_cdc_logement_social IS
    'Caisse des Dépôts: median surfaces and costs of financed social housing, by region and year';

-- -----------------------------------------------------------------------------
-- Accessibility (DAG dvf_accessibilite_pipeline): walking graph, schools,
-- public transport, addresses, and walking / door-to-pole times. Scope:
-- DVF_SCOPE_DEPARTEMENTS (default Gironde). Not part of the semantic layer.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dvf.ref_communes_contours (
    code_commune  VARCHAR(5) PRIMARY KEY,
    nom           TEXT,
    dep_code      VARCHAR(3),
    geom          geometry(MultiPolygon, 2154) NOT NULL
);
CREATE INDEX IF NOT EXISTS ref_communes_contours_geom_idx ON dvf.ref_communes_contours USING gist (geom);

-- Walking graph from OpenStreetMap: vertices are OSM node ids.
CREATE TABLE IF NOT EXISTS dvf.osm_pieton (
    id          BIGINT PRIMARY KEY,
    osm_id      BIGINT NOT NULL,
    source      BIGINT NOT NULL,
    target      BIGINT NOT NULL,
    highway     TEXT,
    longueur_m  DOUBLE PRECISION NOT NULL,
    cout_s      DOUBLE PRECISION NOT NULL,   -- walking seconds at 4.5 km/h (steps x2)
    geom        geometry(LineString, 2154) NOT NULL
);
CREATE INDEX IF NOT EXISTS osm_pieton_source_idx ON dvf.osm_pieton (source);
CREATE INDEX IF NOT EXISTS osm_pieton_target_idx ON dvf.osm_pieton (target);
CREATE INDEX IF NOT EXISTS osm_pieton_geom_idx ON dvf.osm_pieton USING gist (geom);

CREATE TABLE IF NOT EXISTS dvf.osm_pieton_noeuds (
    id          BIGINT PRIMARY KEY,
    composante  BIGINT,                      -- connected component (pgr_connectedComponents)
    principal   BOOLEAN NOT NULL DEFAULT false,  -- in a component big enough to snap to
    geom        geometry(Point, 2154) NOT NULL
);
CREATE INDEX IF NOT EXISTS osm_pieton_noeuds_geom_idx ON dvf.osm_pieton_noeuds USING gist (geom);
CREATE INDEX IF NOT EXISTS osm_pieton_noeuds_principal_idx
    ON dvf.osm_pieton_noeuds USING gist (geom) WHERE principal;

-- Schools (annuaire de l'éducation), with their social and value-added indicators.
CREATE TABLE IF NOT EXISTS dvf.ref_ecoles (
    uai                   VARCHAR(8) PRIMARY KEY,
    nom                   TEXT,
    type                  TEXT NOT NULL,      -- ecole | college | lycee
    maternelle            BOOLEAN,
    elementaire           BOOLEAN,
    lycee_general         BOOLEAN,
    statut                TEXT,               -- Public | Privé
    education_prioritaire TEXT,               -- REP | REP+ | NULL
    code_commune          VARCHAR(5),
    nom_commune           TEXT,
    adresse               TEXT,
    geom                  geometry(Point, 2154),
    noeud                 BIGINT              -- nearest walking-graph vertex
);
ALTER TABLE dvf.ref_ecoles ADD COLUMN IF NOT EXISTS code_nature VARCHAR(3);  -- 340 collège, 390 SEGPA...
ALTER TABLE dvf.ref_ecoles ADD COLUMN IF NOT EXISTS nature TEXT;
CREATE INDEX IF NOT EXISTS ref_ecoles_geom_idx ON dvf.ref_ecoles USING gist (geom);

CREATE TABLE IF NOT EXISTS dvf.ref_ecoles_ips (
    uai                VARCHAR(8) NOT NULL,
    rentree            TEXT NOT NULL,         -- '2024-2025'
    niveau             TEXT NOT NULL,         -- ecole | college
    ips                NUMERIC(6,1),          -- social position index (national mean ~100)
    ips_departemental  NUMERIC(6,1),
    ecart_type         NUMERIC(6,1),
    CONSTRAINT ref_ecoles_ips_pkey PRIMARY KEY (uai, rentree)
);
COMMENT ON TABLE dvf.ref_ecoles_ips IS
    'Indice de position sociale (Education nationale): higher = more favoured families; not a quality score';

CREATE TABLE IF NOT EXISTS dvf.ref_colleges_ivac (
    uai                     VARCHAR(8) NOT NULL,
    session                 SMALLINT NOT NULL,
    nb_candidats            INTEGER,
    taux_reussite           NUMERIC(5,1),     -- brevet, général series
    va_taux_reussite        NUMERIC(5,1),     -- value added: + = better than expected for its pupils
    note_ecrit              NUMERIC(5,1),
    va_note_ecrit           NUMERIC(5,1),
    taux_acces_6e_3e        NUMERIC(5,1),
    CONSTRAINT ref_colleges_ivac_pkey PRIMARY KEY (uai, session)
);

-- Public transport stops (GTFS) and their time to each pole.
CREATE TABLE IF NOT EXISTS dvf.transport_arrets (
    arret     TEXT PRIMARY KEY,               -- '<network>:<stop_id>'
    reseau    TEXT NOT NULL,
    nom       TEXT,
    modes     TEXT[],                         -- tram | bus | train | ferry...
    geom      geometry(Point, 2154) NOT NULL,
    noeud     BIGINT
);
CREATE INDEX IF NOT EXISTS transport_arrets_geom_idx ON dvf.transport_arrets USING gist (geom);

CREATE TABLE IF NOT EXISTS dvf.acces_poles (
    pole              TEXT PRIMARY KEY,
    geom              geometry(Point, 2154) NOT NULL,
    arrivee_debut     TIME NOT NULL DEFAULT '07:30',   -- arrival window on the reference day
    arrivee_fin       TIME NOT NULL DEFAULT '09:00'
);
-- Where people commute to: the centre and the main employment / study hubs.
INSERT INTO dvf.acces_poles (pole, geom)
SELECT pole, ST_Transform(ST_SetSRID(ST_MakePoint(lon, lat), 4326), 2154)
FROM (VALUES
    ('Centre de Bordeaux',          -0.57451, 44.84246),  -- Grand Théâtre
    ('Gare Saint-Jean',             -0.55625, 44.82589),  -- station, Euratlantique business district
    ('Aéroport de Mérignac',        -0.70130, 44.83050),  -- terminal (tram A / bus stops), aeronautics business area
    ('CHU Pellegrin',               -0.60740, 44.82640),  -- main university hospital
    ('Campus Talence-Pessac',       -0.60000, 44.80650)   -- Université de Bordeaux main campus
) AS p (pole, lon, lat)
ON CONFLICT (pole) DO UPDATE SET geom = EXCLUDED.geom;
COMMENT ON TABLE dvf.acces_poles IS
    'Commute destinations: Centre de Bordeaux (Grand Théâtre) and main employment / study hubs';

CREATE TABLE IF NOT EXISTS dvf.transport_temps_pole (
    arret              TEXT NOT NULL,
    pole               TEXT NOT NULL,
    minutes_meilleur   NUMERIC(5,1) NOT NULL,  -- best case over the arrival window
    minutes_typique    NUMERIC(5,1) NOT NULL,  -- median over the arrival window
    jour_reference     DATE NOT NULL,
    CONSTRAINT transport_temps_pole_pkey PRIMARY KEY (arret, pole)
);

-- Addresses (Base Adresse Nationale).
CREATE TABLE IF NOT EXISTS dvf.ref_adresses (
    id             TEXT PRIMARY KEY,
    numero         INTEGER,
    rep            TEXT,
    nom_voie       TEXT,
    code_postal    VARCHAR(5),
    code_commune   VARCHAR(5) NOT NULL,
    nom_commune    TEXT,
    parcelles      TEXT[],                    -- cadastral parcels (same ids as DVF id_parcelle)
    geom           geometry(Point, 2154) NOT NULL,
    noeud          BIGINT,
    acces_m        DOUBLE PRECISION           -- straight distance address -> vertex
);
CREATE INDEX IF NOT EXISTS ref_adresses_geom_idx ON dvf.ref_adresses USING gist (geom);
CREATE INDEX IF NOT EXISTS ref_adresses_commune_idx ON dvf.ref_adresses (code_commune);

-- Walking minutes from each vertex to the nearest school of each level, and
-- door-to-pole minutes (walk + public transport, or walk only).
CREATE TABLE IF NOT EXISTS dvf.acces_noeuds (
    noeud                  BIGINT PRIMARY KEY,
    min_maternelle         NUMERIC(5,1),  uai_maternelle   VARCHAR(8),
    min_elementaire        NUMERIC(5,1),  uai_elementaire  VARCHAR(8),
    min_college            NUMERIC(5,1),  uai_college      VARCHAR(8),
    min_lycee              NUMERIC(5,1),  uai_lycee        VARCHAR(8)
);
CREATE TABLE IF NOT EXISTS dvf.acces_noeuds_poles (
    noeud    BIGINT NOT NULL,
    pole     TEXT NOT NULL,
    minutes  NUMERIC(5,1) NOT NULL,           -- typical door-to-pole time
    CONSTRAINT acces_noeuds_poles_pkey PRIMARY KEY (noeud, pole)
);

-- One row per address: walking minutes to the nearest school of each level
-- (<= 30 min, else NULL) and typical door-to-centre minutes (<= 90 min),
-- including the short walk from the address to its graph vertex.
CREATE OR REPLACE VIEW dvf.v_acces_adresses AS
SELECT a.id, a.numero, a.rep, a.nom_voie, a.code_postal, a.code_commune, a.nom_commune,
       a.parcelles, a.geom,
       round(n.min_maternelle + w.marche, 1)   AS min_maternelle,  n.uai_maternelle,
       round(n.min_elementaire + w.marche, 1)  AS min_elementaire, n.uai_elementaire,
       round(n.min_college + w.marche, 1)      AS min_college,     n.uai_college,
       round(n.min_lycee + w.marche, 1)        AS min_lycee,       n.uai_lycee,
       round(p.minutes + w.marche, 1)          AS min_centre_bordeaux
FROM dvf.ref_adresses a
CROSS JOIN LATERAL (SELECT (coalesce(a.acces_m, 0) * 1.3 / 1.25 / 60)::numeric AS marche) w
LEFT JOIN dvf.acces_noeuds n ON n.noeud = a.noeud
LEFT JOIN dvf.acces_noeuds_poles p ON p.noeud = a.noeud AND p.pole = 'Centre de Bordeaux';

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
