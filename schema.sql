    CREATE SCHEMA IF NOT EXISTS mart;

    -- ============================================================
    -- DIMENSION PARTAGÉE : STATIONS (gares + aéroports)
    -- ============================================================
    CREATE TABLE IF NOT EXISTS mart.dim_station (
    station_id VARCHAR(50) PRIMARY KEY,
    station_name VARCHAR(255) NOT NULL,
    city VARCHAR(255),
    country_code CHAR(2) NOT NULL,
    latitude NUMERIC(9,6),
    longitude NUMERIC(9,6),
    is_airport BOOLEAN NOT NULL DEFAULT FALSE
    );

    -- ============================================================
    -- TRAIN
    -- ============================================================

    -- Routes ferroviaires (gare départ → gare arrivée)
    CREATE TABLE IF NOT EXISTS mart.dim_route_train (
    route_train_id SERIAL PRIMARY KEY,
    dep_station_id VARCHAR(50) NOT NULL,
    arr_station_id VARCHAR(50) NOT NULL,
    distance_km NUMERIC(10,3),
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
    route_avion_id SERIAL PRIMARY KEY,
    dep_station_id VARCHAR(50) NOT NULL,
    arr_station_id VARCHAR(50) NOT NULL,
    distance_km NUMERIC(10,3),
    FOREIGN KEY(dep_station_id) REFERENCES mart.dim_station(station_id),
    FOREIGN KEY(arr_station_id) REFERENCES mart.dim_station(station_id),
    UNIQUE(dep_station_id, arr_station_id)
    );

    -- Types d'avions
    CREATE TABLE IF NOT EXISTS mart.dim_vehicle_avion (
    vehicle_avion_id SERIAL PRIMARY KEY,
    label VARCHAR(100) NOT NULL,
    co2_per_km NUMERIC(10,3) NOT NULL,
    service_type VARCHAR(50) NOT NULL
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