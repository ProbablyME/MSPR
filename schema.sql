    CREATE SCHEMA IF NOT EXISTS mart;

    -- ============================================================
    -- PROVENANCE DES DONNÉES : SOURCES
    -- ============================================================
    CREATE TABLE IF NOT EXISTS mart.dim_source (
    source_id    SERIAL PRIMARY KEY,
    source_name  VARCHAR(200) NOT NULL UNIQUE,  -- ex: 'GTFS - FR - 1234', 'EASA CO2 Database'
    source_type  VARCHAR(50)  NOT NULL,          -- 'gtfs_feed' | 'api' | 'fichier_local'
    url          TEXT,                           -- URL officielle ou chemin de téléchargement
    version      VARCHAR(100),                   -- ex: 'Issue 11', 'v002', '2025-01'
    description  TEXT,                           -- description courte de la source
    loaded_at    TIMESTAMP NOT NULL DEFAULT NOW() -- dernière fois qu'on l'a utilisée
    );

    -- ============================================================
    -- DIMENSION PARTAGÉE : STATIONS (gares + aéroports)
    -- ============================================================
    CREATE TABLE IF NOT EXISTS mart.dim_station (
    station_id VARCHAR(100) PRIMARY KEY,
    station_name VARCHAR(255) NOT NULL,
    city VARCHAR(255),
    country_code CHAR(2) NOT NULL,
    latitude NUMERIC(9,6),
    longitude NUMERIC(9,6),
    is_airport BOOLEAN NOT NULL DEFAULT FALSE,
    source_id INTEGER REFERENCES mart.dim_source(source_id)
    );

    -- ============================================================
    -- TRAIN
    -- ============================================================

    -- Routes ferroviaires (gare départ → gare arrivée)
    CREATE TABLE IF NOT EXISTS mart.dim_route_train (
    route_train_id SERIAL PRIMARY KEY,
    dep_station_id VARCHAR(100) NOT NULL,
    arr_station_id VARCHAR(100) NOT NULL,
    distance_km NUMERIC(10,3),
    is_night_train BOOLEAN NOT NULL DEFAULT FALSE,  -- TRUE = train de nuit (Back on Track)
    source_id INTEGER REFERENCES mart.dim_source(source_id),
    FOREIGN KEY(dep_station_id) REFERENCES mart.dim_station(station_id),
    FOREIGN KEY(arr_station_id) REFERENCES mart.dim_station(station_id),
    UNIQUE(dep_station_id, arr_station_id)
    );

    -- Types de matériel ferroviaire
    CREATE TABLE IF NOT EXISTS mart.dim_vehicle_train (
    vehicle_train_id SERIAL PRIMARY KEY,
    label VARCHAR(100) NOT NULL,
    co2_per_km NUMERIC(10,3) NOT NULL,
    service_type VARCHAR(50) NOT NULL
    );

    -- ============================================================
    -- AVION
    -- ============================================================

    -- Routes aériennes (aéroport départ → aéroport arrivée)
    CREATE TABLE IF NOT EXISTS mart.dim_route_avion (
    route_avion_id    SERIAL PRIMARY KEY,
    dep_station_id    VARCHAR(50)   NOT NULL,
    arr_station_id    VARCHAR(50)   NOT NULL,
    distance_km       NUMERIC(10,3),              -- Distance Haversine (km)
    dominant_typecode VARCHAR(50),                -- Code ICAO type le plus fréquent sur cette route
    co2_total_kg      NUMERIC(12,3),              -- CO2 estimé = distance_km × co2_per_km EASA
    source_id         INTEGER REFERENCES mart.dim_source(source_id),
    FOREIGN KEY(dep_station_id) REFERENCES mart.dim_station(station_id),
    FOREIGN KEY(arr_station_id) REFERENCES mart.dim_station(station_id),
    UNIQUE(dep_station_id, arr_station_id)
    );

    -- Types d'avions (enrichi avec données EASA CO2 Database)
    CREATE TABLE IF NOT EXISTS mart.dim_vehicle_avion (
    vehicle_avion_id SERIAL PRIMARY KEY,
    label            VARCHAR(100) NOT NULL UNIQUE,  -- Désignation EASA (ex: A320-271N)
    co2_per_km       NUMERIC(10,4) NOT NULL,        -- Valeur métrique CO2 EASA (kg/km)
    service_type     VARCHAR(50)  NOT NULL,          -- regional / court_moyen_courrier / long_courrier
    icao_typecode    VARCHAR(10),                    -- Code ICAO type (ex: A20N) pour jointure OPDI
    mtom_kg          INTEGER,                        -- Masse Maximale au Décollage (kg)
    num_engines      SMALLINT,                       -- Nombre de moteurs
    manufacturer     VARCHAR(150),                   -- Titulaire du certificat de type EASA
    source_id        INTEGER REFERENCES mart.dim_source(source_id)
    );

    -- ============================================================
    -- TABLE DE FAITS UNIQUE (train + avion)
    -- ============================================================
    CREATE TABLE IF NOT EXISTS mart.fact_emission (
    fact_id SERIAL PRIMARY KEY,
    transport_mode VARCHAR(10) NOT NULL CHECK (transport_mode IN ('train', 'avion')),

    -- FK train (NULL si avion)
    route_train_id INTEGER,
    vehicle_train_id INTEGER,

    -- FK avion (NULL si train)
    route_avion_id INTEGER,
    vehicle_avion_id INTEGER,

    co2_kg_passenger NUMERIC(10,3) NOT NULL,

    FOREIGN KEY(route_train_id) REFERENCES mart.dim_route_train(route_train_id),
    FOREIGN KEY(vehicle_train_id) REFERENCES mart.dim_vehicle_train(vehicle_train_id),
    FOREIGN KEY(route_avion_id) REFERENCES mart.dim_route_avion(route_avion_id),
    FOREIGN KEY(vehicle_avion_id) REFERENCES mart.dim_vehicle_avion(vehicle_avion_id),

    UNIQUE(route_train_id, vehicle_train_id),
    UNIQUE(route_avion_id, vehicle_avion_id)
    );

    -- ============================================================
    -- MONITORING : HISTORIQUE DES RUNS ETL
    -- ============================================================
    CREATE TABLE IF NOT EXISTS mart.etl_run_log (
    run_id           SERIAL PRIMARY KEY,
    started_at       TIMESTAMP NOT NULL DEFAULT NOW(),
    finished_at      TIMESTAMP,
    duration_s       NUMERIC(10,2),          -- Durée totale en secondes
    mois_traites     INTEGER   DEFAULT 0,    -- Nombre de mois OPDI traités
    airports_charges INTEGER   DEFAULT 0,    -- Aéroports insérés/mis à jour
    routes_chargees  INTEGER   DEFAULT 0,    -- Routes insérées/mises à jour
    co2_exact        INTEGER   DEFAULT 0,    -- Routes avec typecode EASA exact
    co2_fallback     INTEGER   DEFAULT 0,    -- Routes avec CO2 par catégorie ICAO
    erreurs          INTEGER   DEFAULT 0,    -- Nombre d'erreurs non bloquantes
    statut           VARCHAR(20) DEFAULT 'en_cours'  -- en_cours / succes / erreur
    );
