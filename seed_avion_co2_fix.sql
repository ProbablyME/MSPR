-- ============================================================
-- Correction des émissions AVION (appliquée après le seed)
--
-- La base seed historique calculait les émissions avion à partir de la
-- « CO2 Metric Value » EASA, qui est une métrique de CERTIFICATION
-- (efficacité, ICAO Annexe 16 Vol. III) et NON une émission réelle par km :
-- elle sous-estimait les émissions d'un ordre de grandeur (l'avion ressortait
-- à tort plus écologique que le train).
--
-- On recalcule ici avec les facteurs ADEME Base Carbone
-- (kg CO2e / passager.km, hors trainées), cohérents avec le calcul du train.
-- Idempotent : recalcul depuis la distance, rejouable sans effet de bord.
--   regional             : 0.230 kg/pax.km   (70 pax)
--   court_moyen_courrier : 0.178 kg/pax.km  (150 pax)
--   long_courrier        : 0.152 kg/pax.km  (280 pax)
-- ============================================================

BEGIN;

-- 1) co2_total_kg (vol entier) = distance × facteur_pax.km × passagers
UPDATE mart.dim_route_avion r
SET co2_total_kg = ROUND((r.distance_km * ef.tot)::numeric, 3)
FROM (
    SELECT DISTINCT ON (icao_typecode) icao_typecode,
        CASE service_type
            WHEN 'regional'             THEN 0.230 * 70
            WHEN 'court_moyen_courrier' THEN 0.178 * 150
            WHEN 'long_courrier'        THEN 0.152 * 280
        END AS tot
    FROM mart.dim_vehicle_avion
    WHERE icao_typecode IS NOT NULL
    ORDER BY icao_typecode, vehicle_avion_id
) ef
WHERE ef.icao_typecode = r.dominant_typecode
  AND r.distance_km IS NOT NULL;

-- Fallback par tranche de distance pour les types non reconnus
UPDATE mart.dim_route_avion r
SET co2_total_kg = ROUND((r.distance_km * CASE
        WHEN r.distance_km <  500  THEN 0.230 * 70
        WHEN r.distance_km < 4000  THEN 0.178 * 150
        ELSE                            0.152 * 280
    END)::numeric, 3)
WHERE r.dominant_typecode IS NULL
  AND r.distance_km IS NOT NULL;

-- 2) co2_kg_passenger (par passager) = distance × facteur_pax.km de la catégorie
UPDATE mart.fact_emission fe
SET co2_kg_passenger = ROUND((ra.distance_km * CASE v.service_type
        WHEN 'regional'             THEN 0.230
        WHEN 'court_moyen_courrier' THEN 0.178
        WHEN 'long_courrier'        THEN 0.152
    END)::numeric, 3)
FROM mart.dim_route_avion ra, mart.dim_vehicle_avion v
WHERE fe.transport_mode = 'avion'
  AND fe.route_avion_id = ra.route_avion_id
  AND fe.vehicle_avion_id = v.vehicle_avion_id
  AND ra.distance_km IS NOT NULL;

COMMIT;
