-- =============================================================================
-- DVF semantic layer: the ONLY objects a text-to-SQL system should query.
--
-- Rebuilt by the DAG task `refresh_semantic_layer` after every committed load
-- (idempotent, one transaction). Everything here is DERIVED from dvf.mutations
-- or static reference data, so dropping and rebuilding it never loses data.
--
-- Scope: Gironde (code_departement = '33'). dvf.mutations keeps all of France;
-- change SCOPE in the `lignes` CTEs below to widen the semantic layer.
--
-- Conventions (documented once, used by every gold query):
--   * A "vente" is one mutation (id_mutation), never one raw row.
--     est_vente = nature_mutation in (Vente, VEFA, Vente terrain à bâtir,
--     Adjudication); Echange and Expropriation are mutations but not sales.
--   * A "bien" is one deduplicated property. DVF repeats a local once per
--     nature de culture of its parcel; dvf.biens keeps one row per real local.
--   * "Vente simple" (bien_unique): exactly one Maison or Appartement, no
--     commercial premises; dépendances (cave, parking) are allowed.
--   * Price statistics use vente_reference = Vente or VEFA, bien_unique,
--     valeur_fonciere > 0. prix_m2 = valeur_fonciere / surface_reelle_bati on
--     those sales only (NULL otherwise).
--   * "prix" without qualifier = median. T<n> = n pièces principales.
--   * VEFA TRAP: ~95% of VEFA sales carry no type_local and no surface (lots of
--     a building not yet in the cadastre): type_bien = 'Lot non décrit'. They
--     count as ventes, but never as apartments, and have no prix_m2.
--   * Spatial: every `geom` here is a Point in Lambert-93 (EPSG:2154), whose
--     unit is the METRE. ST_DWithin(a.geom, b.geom, 500) = within 500 m, and
--     it uses the GiST indexes. (Scale error in Gironde < 0.1%.) Build ad-hoc
--     points with ST_Transform(ST_SetSRID(ST_MakePoint(lon, lat), 4326), 2154).
--     Rows without coordinates have geom NULL and drop out of spatial queries.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Reference data (static, upserted)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dvf.lieux (
    nom        TEXT PRIMARY KEY,
    longitude  DOUBLE PRECISION NOT NULL,
    latitude   DOUBLE PRECISION NOT NULL,
    geom       geometry(Point, 2154)
               GENERATED ALWAYS AS (ST_Transform(ST_Point(longitude, latitude, 4326), 2154)) STORED,
    source     TEXT
);
COMMENT ON TABLE dvf.lieux IS 'Named landmarks for distance questions (WGS84 input, approximate).';

INSERT INTO dvf.lieux (nom, longitude, latitude, source) VALUES
    ('Place de la Bourse',               -0.56950, 44.84125, 'OSM, centre of the square'),
    ('Gare Saint-Jean',                  -0.55625, 44.82589, 'OSM, main building'),
    ('Place des Quinconces',             -0.57367, 44.84497, 'OSM, centre of the square'),
    ('Aéroport Bordeaux-Mérignac',       -0.71556, 44.82833, 'AIP LFBD aerodrome reference point'),
    ('Plage centrale de Lacanau-Océan',  -1.20300, 45.00100, 'OSM, central beach access')
ON CONFLICT (nom) DO UPDATE
SET longitude = EXCLUDED.longitude, latitude = EXCLUDED.latitude, source = EXCLUDED.source;

CREATE TABLE IF NOT EXISTS dvf.zones (
    zone          TEXT        NOT NULL,
    code_commune  VARCHAR(5)  NOT NULL,
    PRIMARY KEY (zone, code_commune)
);
COMMENT ON TABLE dvf.zones IS 'Groups of communes that are not in DVF (intercommunalités, bassins).';

INSERT INTO dvf.zones (zone, code_commune)
SELECT 'Bordeaux Métropole', unnest(ARRAY[
    '33003','33004','33013','33032','33039','33056','33063','33065','33069','33075',
    '33096','33119','33162','33167','33192','33200','33249','33273','33281','33312',
    '33318','33376','33434','33449','33487','33519','33522','33550'])
UNION ALL
SELECT 'Bassin d''Arcachon', unnest(ARRAY[
    '33009','33529','33199','33527','33051','33019','33229','33005','33011','33236'])
ON CONFLICT DO NOTHING;

-- -----------------------------------------------------------------------------
-- Derived objects: dropped and rebuilt (definition changes apply on next load)
-- -----------------------------------------------------------------------------
-- dvf.ventes is built from dvf.biens, and both read names from dvf.communes:
-- drop dependents first. Before the official list of communes, dvf.communes
-- was a plain VIEW built ON dvf.ventes, so that old kind goes first.
DO $$
BEGIN
    IF (SELECT relkind FROM pg_class WHERE oid = to_regclass('dvf.communes')) = 'v' THEN
        DROP VIEW dvf.communes;
    END IF;
END
$$;
DROP MATERIALIZED VIEW IF EXISTS dvf.ventes;
DROP MATERIALIZED VIEW IF EXISTS dvf.biens;
DROP MATERIALIZED VIEW IF EXISTS dvf.communes;

-- dvf.communes: ONE row and ONE canonical name per commune code of the scope.
-- Names come from the official list (dvf.ref_communes); a code missing from it
-- (a commune merged since, e.g. 33409 Saint-Genis-du-Bois) keeps its DVF name.
-- Postal codes come from DVF itself: the official list also holds CEDEX codes
-- (33001, 33012...), which look like INSEE codes and only cause confusion.
CREATE MATERIALIZED VIEW dvf.communes AS
WITH dvf_noms AS (
    SELECT DISTINCT ON (code_commune) code_commune, nom_commune
    FROM (SELECT code_commune, nom_commune, count(*) AS n
          FROM dvf.mutations WHERE code_departement = '33'  -- SCOPE
          GROUP BY 1, 2) AS t
    ORDER BY code_commune, n DESC, nom_commune
),
postaux AS (
    SELECT code_commune, array_agg(DISTINCT code_postal ORDER BY code_postal) AS codes_postaux
    FROM dvf.mutations
    WHERE code_departement = '33' AND code_postal IS NOT NULL  -- SCOPE
    GROUP BY code_commune
),
ref AS (
    SELECT * FROM dvf.ref_communes WHERE dep_code = '33'  -- SCOPE
)
SELECT
    coalesce(r.code_insee, d.code_commune)                        AS code_commune,
    coalesce(r.nom, d.nom_commune)                                AS nom_commune,
    CASE WHEN d.nom_commune <> r.nom THEN d.nom_commune END       AS nom_dans_dvf,
    coalesce(r.nom_sans_accent, lower(d.nom_commune))             AS nom_sans_accent,
    pc.codes_postaux,
    r.epci_code,
    r.epci_nom,
    r.population,
    r.superficie_km2,
    r.densite,
    r.grille_densite,
    r.latitude,
    r.longitude
FROM ref AS r
FULL JOIN dvf_noms AS d ON d.code_commune = r.code_insee
LEFT JOIN postaux AS pc ON pc.code_commune = coalesce(r.code_insee, d.code_commune);

COMMENT ON MATERIALIZED VIEW dvf.communes IS
    'Communes of the scope (Gironde): one row and one canonical name per INSEE code. '
    'Look up code_commune here, never type an INSEE code from memory. '
    'Postal codes (codes_postaux) are NOT INSEE codes.';
COMMENT ON COLUMN dvf.communes.code_commune IS 'INSEE code (5 characters), the key used by dvf.ventes and dvf.biens';
COMMENT ON COLUMN dvf.communes.nom_commune IS 'Official current name (dvf.ventes and dvf.biens use the same)';
COMMENT ON COLUMN dvf.communes.nom_dans_dvf IS 'Older name found in DVF rows when it differs (e.g. Cadillac)';
COMMENT ON COLUMN dvf.communes.nom_sans_accent IS 'Lower-case name without accents, for fuzzy matching';
COMMENT ON COLUMN dvf.communes.codes_postaux IS 'Postal codes seen in DVF for this commune';
COMMENT ON COLUMN dvf.communes.epci_nom IS 'Intercommunality (e.g. Bordeaux Métropole)';
COMMENT ON COLUMN dvf.communes.population IS 'Population (official list edition, see dvf.ref_communes)';
COMMENT ON COLUMN dvf.communes.latitude IS 'Centre of the commune, WGS84';

CREATE UNIQUE INDEX communes_code_idx ON dvf.communes (code_commune);

-- dvf.biens: one row per deduplicated property (local) of a mutation.
CREATE MATERIALIZED VIEW dvf.biens AS
WITH lignes AS (
    SELECT * FROM dvf.mutations WHERE code_departement = '33'  -- SCOPE
),
par_culture AS (
    -- A local appears once per (nature de culture) of its parcel: count its
    -- rows within each culture, then keep the max across cultures. Identical
    -- locals sold together (e.g. 8 studios of 33 m²) are kept as 8 biens.
    SELECT annee, id_mutation, id_parcelle, code_type_local, surface_reelle_bati,
           nombre_pieces_principales, lot1_numero, lot1_surface_carrez,
           count(*) AS n, min(num_ligne_source) AS ligne
    FROM lignes
    WHERE code_type_local IS NOT NULL
    GROUP BY annee, id_mutation, id_parcelle, code_type_local, surface_reelle_bati,
             nombre_pieces_principales, lot1_numero, lot1_surface_carrez,
             code_nature_culture, code_nature_culture_speciale
),
signatures AS (
    SELECT annee, id_mutation, id_parcelle, code_type_local, surface_reelle_bati,
           nombre_pieces_principales, lot1_numero, lot1_surface_carrez,
           max(n) AS n, min(ligne) AS ligne
    FROM par_culture
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
)
SELECT
    l.annee,
    l.id_mutation,
    l.date_mutation,
    l.nature_mutation,
    l.code_commune,
    coalesce(c.nom_commune, l.nom_commune) AS nom_commune,
    l.code_postal,
    l.adresse_numero,
    l.adresse_suffixe,
    l.adresse_nom_voie,
    l.id_parcelle,
    l.code_type_local,
    l.type_local,
    l.surface_reelle_bati,
    l.nombre_pieces_principales,
    l.lot1_numero,
    l.lot1_surface_carrez,
    l.nature_mutation IN ('Vente', 'Vente en l''état futur d''achèvement',
                          'Vente terrain à bâtir', 'Adjudication') AS est_vente,
    l.longitude,
    l.latitude,
    ST_Transform(l.geom, 2154)::geometry(Point, 2154) AS geom
FROM signatures AS s
JOIN lignes AS l ON l.annee = s.annee AND l.num_ligne_source = s.ligne
LEFT JOIN dvf.communes AS c ON c.code_commune = l.code_commune
CROSS JOIN LATERAL generate_series(1, s.n);

COMMENT ON MATERIALIZED VIEW dvf.biens IS
    'One row per property sold (deduplicated). Count properties here; prices live in dvf.ventes.';

CREATE INDEX biens_commune_annee_idx ON dvf.biens (code_commune, annee);
CREATE INDEX biens_id_mutation_idx   ON dvf.biens (id_mutation);
CREATE INDEX biens_geom_idx          ON dvf.biens USING gist (geom);

-- dvf.ventes: one row per mutation, price stated once.
CREATE MATERIALIZED VIEW dvf.ventes AS
WITH lignes AS (
    SELECT * FROM dvf.mutations WHERE code_departement = '33'  -- SCOPE
),
mutation AS (
    SELECT annee, id_mutation,
           min(date_mutation)            AS date_mutation,
           min(nature_mutation)          AS nature_mutation,
           max(valeur_fonciere)          AS valeur_fonciere,
           count(*)                      AS nb_lignes,
           count(DISTINCT id_parcelle)   AS nb_parcelles,
           bool_or(lot1_numero IS NOT NULL) AS a_des_lots,
           min(num_ligne_source)         AS premiere_ligne,
           avg(longitude)                AS longitude_moy,
           avg(latitude)                 AS latitude_moy,
           ST_Centroid(ST_Collect(geom)) AS geom_centre
    FROM lignes
    GROUP BY annee, id_mutation
),
terrain AS (
    -- surface_terrain repeats on every local of the parcel: sum it once per
    -- (parcel, culture).
    SELECT id_mutation, sum(surface_terrain) AS surface_terrain
    FROM (SELECT DISTINCT id_mutation, id_parcelle, code_nature_culture,
                 code_nature_culture_speciale, surface_terrain
          FROM lignes WHERE surface_terrain IS NOT NULL) AS t
    GROUP BY id_mutation
),
locaux AS (
    SELECT id_mutation,
           count(*)                                            AS nb_biens,
           count(*) FILTER (WHERE code_type_local = 1)         AS nb_maisons,
           count(*) FILTER (WHERE code_type_local = 2)         AS nb_appartements,
           count(*) FILTER (WHERE code_type_local = 3)         AS nb_dependances,
           count(*) FILTER (WHERE code_type_local = 4)         AS nb_locaux_activite,
           sum(surface_reelle_bati)                            AS surface_bati_totale,
           -- the main property: the house/apartment if any, else the first local
           (array_agg(b ORDER BY (code_type_local IN (1, 2)) DESC, code_type_local))[1] AS principal
    FROM dvf.biens AS b
    GROUP BY id_mutation
)
SELECT
    m.annee,
    m.id_mutation,
    m.date_mutation,
    m.nature_mutation,
    m.valeur_fonciere,
    -- code and name always describe the SAME commune: the main property's,
    -- else the first DVF row's (a land-only sale over two communes used to
    -- get the smallest code with another row's name).
    coalesce((lo.principal).code_commune, fl.code_commune)      AS code_commune,
    coalesce(cn.nom_commune, (lo.principal).nom_commune, fl.nom_commune) AS nom_commune,
    coalesce((lo.principal).code_postal, fl.code_postal)        AS code_postal,
    coalesce((lo.principal).adresse_numero, fl.adresse_numero)  AS adresse_numero,
    coalesce((lo.principal).adresse_nom_voie, fl.adresse_nom_voie) AS adresse_nom_voie,
    CASE
        -- Copropriété lots with no described local: mostly VEFA (the building
        -- doesn't exist yet in the cadastre), no type and no surface.
        WHEN coalesce(lo.nb_biens, 0) = 0 AND m.a_des_lots THEN 'Lot non décrit'
        WHEN coalesce(lo.nb_biens, 0) = 0 THEN 'Terrain'
        WHEN lo.nb_maisons + lo.nb_appartements + lo.nb_locaux_activite = 0 THEN 'Dépendance'
        WHEN lo.nb_maisons > 0 AND lo.nb_appartements = 0 AND lo.nb_locaux_activite = 0 THEN 'Maison'
        WHEN lo.nb_appartements > 0 AND lo.nb_maisons = 0 AND lo.nb_locaux_activite = 0 THEN 'Appartement'
        WHEN lo.nb_locaux_activite > 0 AND lo.nb_maisons + lo.nb_appartements = 0 THEN 'Local'
        ELSE 'Mixte'
    END                                                         AS type_bien,
    coalesce(lo.nb_biens, 0)                                    AS nb_biens,
    coalesce(lo.nb_maisons, 0)                                  AS nb_maisons,
    coalesce(lo.nb_appartements, 0)                             AS nb_appartements,
    coalesce(lo.nb_dependances, 0)                              AS nb_dependances,
    coalesce(lo.nb_locaux_activite, 0)                          AS nb_locaux_activite,
    m.nb_lignes,
    m.nb_parcelles,
    flags.bien_unique,
    CASE WHEN flags.bien_unique THEN (lo.principal).surface_reelle_bati END        AS surface_reelle_bati,
    CASE WHEN flags.bien_unique THEN (lo.principal).nombre_pieces_principales END  AS nombre_pieces,
    CASE WHEN flags.bien_unique THEN (lo.principal).lot1_surface_carrez END        AS surface_carrez,
    lo.surface_bati_totale,
    te.surface_terrain,
    flags.est_vente,
    flags.vente_reference,
    CASE WHEN flags.vente_reference AND (lo.principal).surface_reelle_bati > 0
         THEN round(m.valeur_fonciere / (lo.principal).surface_reelle_bati, 2) END AS prix_m2,
    coalesce((lo.principal).longitude, m.longitude_moy)         AS longitude,
    coalesce((lo.principal).latitude, m.latitude_moy)           AS latitude,
    -- the main property's point, else the centroid of the mutation's parcels
    coalesce((lo.principal).geom, ST_Transform(m.geom_centre, 2154))::geometry(Point, 2154) AS geom
FROM mutation AS m
JOIN lignes AS fl ON fl.annee = m.annee AND fl.num_ligne_source = m.premiere_ligne
LEFT JOIN locaux AS lo ON lo.id_mutation = m.id_mutation
LEFT JOIN dvf.communes AS cn ON cn.code_commune = coalesce((lo.principal).code_commune, fl.code_commune)
LEFT JOIN terrain AS te ON te.id_mutation = m.id_mutation
CROSS JOIN LATERAL (
    SELECT
        coalesce(lo.nb_maisons + lo.nb_appartements = 1 AND lo.nb_locaux_activite = 0, false) AS bien_unique,
        m.nature_mutation IN ('Vente', 'Vente en l''état futur d''achèvement',
                              'Vente terrain à bâtir', 'Adjudication')                        AS est_vente
) AS f0
CROSS JOIN LATERAL (
    SELECT f0.bien_unique, f0.est_vente,
           f0.bien_unique
           AND m.nature_mutation IN ('Vente', 'Vente en l''état futur d''achèvement')
           AND m.valeur_fonciere > 0                                                         AS vente_reference
) AS flags;

COMMENT ON MATERIALIZED VIEW dvf.ventes IS
    'One row per mutation (Gironde). valeur_fonciere stated ONCE: safe to SUM/AVG. '
    'prix_m2 is set only for vente_reference rows (single house/apartment, Vente or VEFA).';
COMMENT ON COLUMN dvf.ventes.type_bien IS
    'Maison | Appartement | Local | Dépendance | Terrain | Lot non décrit | Mixte (from deduplicated biens). '
    'Lot non décrit = copropriété lots without type or surface, ~95% of VEFA sales.';
COMMENT ON COLUMN dvf.ventes.bien_unique IS
    'Exactly one Maison or Appartement and no commercial premises (dépendances allowed)';
COMMENT ON COLUMN dvf.ventes.est_vente IS
    'Vente, VEFA, Vente terrain à bâtir or Adjudication (excludes Echange, Expropriation)';
COMMENT ON COLUMN dvf.ventes.vente_reference IS
    'Used for price statistics: bien_unique, nature Vente or VEFA, valeur_fonciere > 0';

CREATE UNIQUE INDEX ventes_id_mutation_idx ON dvf.ventes (id_mutation);
CREATE INDEX ventes_commune_annee_idx      ON dvf.ventes (code_commune, annee);
CREATE INDEX ventes_annee_idx              ON dvf.ventes (annee);
CREATE INDEX ventes_geom_idx               ON dvf.ventes USING gist (geom);

ANALYZE dvf.communes;
ANALYZE dvf.biens;
ANALYZE dvf.ventes;
