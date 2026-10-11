-- =============================================================================
-- Semantic layer, part 2: housing and living-environment data next to DVF.
-- Plain views over the reference tables (loaded by dvf_loyers_pipeline,
-- dvf_logement_pipeline and dvf_accessibilite_pipeline), scoped to Gironde,
-- all linked to dvf.communes by code_commune. Applied after sql/semantic.sql.
-- They never touch dvf.ventes / dvf.biens: creating them can't change a
-- result of the eval set.
-- =============================================================================

CREATE INDEX IF NOT EXISTS ref_loyers_dep_idx ON dvf.ref_loyers (dep_code, annee);
CREATE INDEX IF NOT EXISTS ref_lovac_code_idx ON dvf.ref_lovac (niveau, code);

CREATE OR REPLACE VIEW dvf.loyers AS
SELECT annee, code_insee AS code_commune, segment, loyer_m2, loyer_m2_bas, loyer_m2_haut,
       niveau_prediction, nb_obs_commune, r2_ajuste,
       (coalesce(nb_obs_commune, 0) >= 30 AND coalesce(r2_ajuste, 0) >= 0.5) AS fiable
FROM dvf.ref_loyers
WHERE dep_code = '33';  -- SCOPE
COMMENT ON VIEW dvf.loyers IS
    'Carte des loyers (ANIL): PREDICTED advertised rent, EUR/m2 charges included, unfurnished '
    'reference dwelling advertised in Q3 of annee. Not a median nor a mean. Years 2022-2025 '
    '(no 2021). Editions are NOT comparable over time. Credit: Estimations ANIL, à partir des '
    'données du Groupe SeLoger et de leboncoin';
COMMENT ON COLUMN dvf.loyers.segment IS
    'appartement | appartement_1_2_pieces | appartement_3_pieces_plus | maison';
COMMENT ON COLUMN dvf.loyers.fiable IS
    'false when fewer than 30 ads in the commune or adjusted R2 < 0.5 (source guidance)';

CREATE OR REPLACE VIEW dvf.logements_sociaux AS
SELECT millesime AS annee, code_commune, code_iris, qpv, type_construction, nb_pieces,
       surface_habitable, annee_construction, annee_mise_en_location, financement,
       dpe_energie, dpe_ges, geom
FROM dvf.ref_rpls
WHERE dep_code = '33';  -- SCOPE
COMMENT ON VIEW dvf.logements_sociaux IS
    'RPLS: one row per social dwelling at 1 January of annee (2022-2025, none before)';
COMMENT ON COLUMN dvf.logements_sociaux.qpv IS 'In a priority neighbourhood of the city policy (QPV): Oui / Non';
COMMENT ON COLUMN dvf.logements_sociaux.type_construction IS 'individuel | collectif';

CREATE OR REPLACE VIEW dvf.logements_vacants AS
SELECT millesime AS annee, niveau, code, libelle, parc_prive, vacants, vacants_secret,
       vacants_plus_2ans, vacants_plus_2ans_secret
FROM dvf.ref_lovac
WHERE (niveau = 'commune' AND code LIKE '33%')  -- SCOPE
   OR (niveau = 'departement' AND code = '33')
   OR (niveau = 'region' AND code = '75')
   OR niveau = 'france';
COMMENT ON VIEW dvf.logements_vacants IS
    'LOVAC vacant PRIVATE dwellings, editions 2020-2026 (edition N ~ situation at 1 January N-1). '
    'Commune values withheld by statistical secret are NULL (*_secret = true), so communes do not '
    'add up: use niveau = departement for Gironde totals. Method break in 2025. No parc_prive in 2026.';
COMMENT ON COLUMN dvf.logements_vacants.code IS 'INSEE code of the commune (niveau commune), 33, 75 or FR';

CREATE OR REPLACE VIEW dvf.series_longues AS
SELECT pays, categorie, serie, serie_en, annee, valeur FROM dvf.ref_series_longues;
COMMENT ON VIEW dvf.series_longues IS
    'IGEDD long series 1800-2020 (France, Etats-Unis, Royaume-Uni). Amounts in EUR even before '
    '1999 (1 EUR = 6.55957 FRF); indices on base 1 in 2000; interest rates as FRACTIONS '
    '(0.05 = 5 %); latest years may be provisional';

CREATE OR REPLACE VIEW dvf.cout_logement_social AS
SELECT code_region, region, annee, construction_surface_m2, construction_cout_m2,
       construction_cout_logement, rehabilitation_surface_m2, rehabilitation_cout_m2,
       rehabilitation_cout_logement
FROM dvf.ref_cdc_logement_social;
COMMENT ON VIEW dvf.cout_logement_social IS
    'Caisse des Dépôts: MEDIAN surface and cost price of financed social housing, by REGION '
    '(Gironde = Nouvelle-Aquitaine, code 75) and signing year, 2018-2025';

CREATE OR REPLACE VIEW dvf.ecoles AS
SELECT uai, nom, type, nature, statut, maternelle, elementaire, lycee_general,
       education_prioritaire, code_commune, adresse, geom,
       (type = 'college' AND code_nature = '340') OR (type = 'ecole' AND code_nature <> '170')
       OR (type = 'lycee' AND code_nature IN ('300', '301', '302', '306', '307')) AS choix_famille
FROM dvf.ref_ecoles;
COMMENT ON VIEW dvf.ecoles IS
    'Schools of Gironde (annuaire de l''éducation). type: ecole | college | lycee';
COMMENT ON COLUMN dvf.ecoles.choix_famille IS
    'false for SEGPA sections, sections attached to a lycée, vocational lycées and schools '
    'without pupils: not counted as "the nearest school"';

CREATE OR REPLACE VIEW dvf.ecoles_ips AS
SELECT uai, rentree, niveau, ips, ips_departemental, ecart_type FROM dvf.ref_ecoles_ips;
COMMENT ON VIEW dvf.ecoles_ips IS
    'Social position index of schools (IPS): higher = more favoured families. A social profile, '
    'NOT a quality score. NULL when the school is too small (NS)';

CREATE OR REPLACE VIEW dvf.colleges_ivac AS
SELECT uai, session, nb_candidats, taux_reussite, va_taux_reussite, note_ecrit, va_note_ecrit,
       taux_acces_6e_3e
FROM dvf.ref_colleges_ivac;
COMMENT ON VIEW dvf.colleges_ivac IS
    'Collège value added (IVAC), brevet sessions 2022-2025: va_* > 0 = better than expected given '
    'the pupils';

DROP VIEW IF EXISTS dvf.arrets_transport;
CREATE VIEW dvf.arrets_transport AS
SELECT a.arret, a.reseau, a.nom, a.modes, c.code_commune, a.geom, t.minutes_meilleur,
       t.minutes_typique, t.jour_reference
FROM dvf.transport_arrets a
LEFT JOIN dvf.ref_communes_contours c ON ST_Contains(c.geom, a.geom)
LEFT JOIN dvf.transport_temps_pole t ON t.arret = a.arret AND t.pole = 'Centre de Bordeaux';
COMMENT ON COLUMN dvf.arrets_transport.code_commune IS 'Commune of the stop (NULL outside Gironde)';
COMMENT ON VIEW dvf.arrets_transport IS
    'Public transport stops (TBM, TransGironde, SNCF) and the time from each stop to the centre '
    'of Bordeaux (Grand Théâtre) for an arrival 07:30-09:00 on a reference Tuesday: best case and '
    'typical (median). The wait before the first departure is not counted';

CREATE OR REPLACE VIEW dvf.dpe AS
SELECT numero_dpe, date_dpe, etiquette_dpe, etiquette_ges, type_batiment, methode,
       periode_construction, annee_construction, typologie, surface_m2, etage,
       energie_chauffage, cout_annuel_eur, conso_ep_m2, code_commune, geom
FROM dvf.ref_dpe;
COMMENT ON VIEW dvf.dpe IS
    'ADEME energy performance certificates of existing dwellings, since July 2021 (current ones '
    'only). TRAP: ~21% of apartment certificates are copied from the building''s '
    '(methode = ''dpe appartement généré à partir des données DPE immeuble''). Rental bans: G since '
    '2025, F from 2028, E from 2034. cout_annuel_eur = conventional estimate, not a bill';

CREATE OR REPLACE VIEW dvf.temps_trajet AS
SELECT a.id AS adresse_id, a.code_commune, p.pole,
       round(p.minutes + (coalesce(a.acces_m, 0) * 1.3 / 1.25 / 60)::numeric, 1) AS minutes
FROM dvf.ref_adresses a
JOIN dvf.acces_noeuds_poles p ON p.noeud = a.noeud;
COMMENT ON VIEW dvf.temps_trajet IS
    'Typical door-to-destination minutes (walk + public transport, arrival 07:30-09:00 on a '
    'reference Tuesday; walking only when faster), per address and destination (pole): '
    'Centre de Bordeaux, Gare Saint-Jean, Aéroport de Mérignac, CHU Pellegrin, Campus '
    'Talence-Pessac. No row = more than 90 minutes. Join dvf.acces_adresses on adresse_id = id';

CREATE OR REPLACE VIEW dvf.acces_adresses AS
SELECT id, numero, rep, nom_voie, code_postal, code_commune, nom_commune AS commune, geom,
       min_maternelle, min_elementaire, min_college, min_lycee, min_centre_bordeaux,
       uai_maternelle, uai_elementaire, uai_college, uai_lycee
FROM dvf.v_acces_adresses;
COMMENT ON VIEW dvf.acces_adresses IS
    'One row per address (Base Adresse Nationale): WALKING minutes to the nearest school of each '
    'level (NULL beyond 30 min) and typical door-to-centre-of-Bordeaux minutes by walking + public '
    'transport (NULL beyond 90 min)';

-- -----------------------------------------------------------------------------
-- Fuzzy commune search: the way to turn what a user typed into a code_commune.
-- -----------------------------------------------------------------------------
-- 'Saint-Médard-d''Eyrans' -> 'saint medard d eyrans'; 'St Emilion' -> 'saint emilion'
CREATE OR REPLACE FUNCTION dvf.normaliser_nom(nom text) RETURNS text
LANGUAGE sql STABLE PARALLEL SAFE AS $fn$
    SELECT btrim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
               ' ' || lower(public.unaccent(coalesce(nom, ''))) || ' ',
               '[-''’`.,()/]', ' ', 'g'),
               '\s+', ' ', 'g'),
               ' ste ', ' sainte ', 'g'),
               ' st ', ' saint ', 'g'))
$fn$;

CREATE OR REPLACE FUNCTION dvf.sans_article(nom text) RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $fn$
    SELECT regexp_replace(nom, '^(le|la|les|l) ', '')
$fn$;

CREATE OR REPLACE FUNCTION dvf.chercher_commune(recherche text)
RETURNS TABLE (code_commune varchar, nom_commune text, correspondance text, score real)
LANGUAGE sql STABLE AS $fn$
    WITH q AS (
        SELECT dvf.normaliser_nom(recherche) AS n, dvf.sans_article(dvf.normaliser_nom(recherche)) AS na
    ), c AS (
        SELECT c.code_commune, c.nom_commune,
               dvf.normaliser_nom(c.nom_commune) AS n,
               dvf.sans_article(dvf.normaliser_nom(c.nom_commune)) AS na,
               dvf.sans_article(dvf.normaliser_nom(c.nom_dans_dvf)) AS na_dvf
        FROM dvf.communes c
    ), m AS (
        SELECT c.code_commune, c.nom_commune,
               CASE
                   WHEN c.na = q.na OR c.na_dvf = q.na THEN 1   -- exact (accents, case, articles, St/Saint ignored)
                   WHEN c.na LIKE q.na || ' %' THEN 2            -- starts with: 'saint medard' -> 3 communes
                   WHEN similarity(c.na, q.na) >= 0.35 THEN 3    -- typo: 'bordeau', 'merignak'
               END AS rang,
               similarity(c.na, q.na) AS sim
        FROM c, q
    )
    SELECT m.code_commune, m.nom_commune,
           (ARRAY['exacte', 'debut du nom', 'approchante'])[m.rang] AS correspondance,
           round((CASE m.rang WHEN 1 THEN 1 WHEN 2 THEN 0.9 ELSE m.sim END)::numeric, 2)::real AS score
    FROM m
    WHERE m.rang = (SELECT min(rang) FROM m)   -- only the best kind of match found
    ORDER BY m.sim DESC, m.nom_commune
    LIMIT 10
$fn$;

COMMENT ON FUNCTION dvf.chercher_commune(text) IS
    'Fuzzy commune search: SELECT * FROM dvf.chercher_commune(''bordeau''). Ignores case, accents, '
    'hyphens, apostrophes, St/Ste and leading articles; tolerates typos. Returns only the best kind '
    'of match: exacte, else debut du nom (several rows = ambiguous: ask), else approchante.';
