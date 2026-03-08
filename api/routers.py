from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List, Optional
from pydantic import BaseModel
from database import get_db

router = APIRouter()

# ============================================================
# PYDANTIC MODELS
# ============================================================

# --- Stations ---

class StationResponse(BaseModel):
    station_id: str
    station_name: str
    city: Optional[str]
    country_code: str
    latitude: Optional[float]
    longitude: Optional[float]
    is_airport: bool
    source_id: Optional[int]

class StationNearbyResponse(StationResponse):
    distance_km: float

class PaginatedStations(BaseModel):
    page: int
    per_page: int
    total: int
    data: List[StationResponse]

# --- Routes Train ---

class RouteTrainResponse(BaseModel):
    route_train_id: int
    dep_station_id: str
    dep_station_name: Optional[str]
    dep_city: Optional[str]
    arr_station_id: str
    arr_station_name: Optional[str]
    arr_city: Optional[str]
    distance_km: Optional[float]
    is_night_train: bool

class PaginatedRoutesTrain(BaseModel):
    page: int
    per_page: int
    total: int
    data: List[RouteTrainResponse]

# --- Routes Avion ---

class RouteAvionResponse(BaseModel):
    route_avion_id: int
    dep_station_id: str
    dep_station_name: Optional[str]
    dep_city: Optional[str]
    arr_station_id: str
    arr_station_name: Optional[str]
    arr_city: Optional[str]
    distance_km: Optional[float]
    dominant_typecode: Optional[str]
    co2_total_kg: Optional[float]

class PaginatedRoutesAvion(BaseModel):
    page: int
    per_page: int
    total: int
    data: List[RouteAvionResponse]

# --- Vehicles ---

class VehicleTrainResponse(BaseModel):
    vehicle_train_id: int
    label: str
    co2_per_km: float
    service_type: str

class VehicleAvionResponse(BaseModel):
    vehicle_avion_id: int
    label: str
    co2_per_km: float
    service_type: str
    icao_typecode: Optional[str]
    mtom_kg: Optional[int]
    num_engines: Optional[int]
    manufacturer: Optional[str]

# --- Emissions ---

class EmissionResponse(BaseModel):
    fact_id: int
    transport_mode: str
    route_train_id: Optional[int]
    vehicle_train_id: Optional[int]
    route_avion_id: Optional[int]
    vehicle_avion_id: Optional[int]
    co2_kg_passenger: float
    dep_city: Optional[str]
    arr_city: Optional[str]

class PaginatedEmissions(BaseModel):
    page: int
    per_page: int
    total: int
    data: List[EmissionResponse]

# --- Compare ---

class CompareResponse(BaseModel):
    mode: str
    dep_city: str
    arr_city: str
    distance_km: float
    co2_per_km: float
    co2_kg_passenger: float

class WinnerResponse(BaseModel):
    greener_mode: str
    dep_city: str
    arr_city: str
    train_co2_kg: Optional[float]
    plane_co2_kg: Optional[float]
    savings_kg: Optional[float]
    savings_percent: Optional[float]

class PassengersResponse(BaseModel):
    mode: str
    dep_city: str
    arr_city: str
    passengers: int
    total_co2_kg: float
    co2_per_passenger_kg: float
    equivalent_trees: float
    equivalent_car_km: float

# --- Search ---

class CitySearchResponse(BaseModel):
    city: str
    country_code: str
    has_train: bool
    has_airport: bool

class DepartureResponse(BaseModel):
    arr_city: str
    distance_km: Optional[float]
    train_co2_kg: Optional[float]
    plane_co2_kg: Optional[float]
    greener_mode: Optional[str]

# --- Rankings ---

class RouteRankingResponse(BaseModel):
    dep_city: str
    arr_city: str
    train_co2_kg: float
    plane_co2_kg: float
    savings_kg: float
    savings_percent: float

class LongestRouteResponse(BaseModel):
    dep_city: str
    arr_city: str
    distance_km: float
    transport_mode: str

# --- Stats ---

class NetworkStatsResponse(BaseModel):
    total_train_routes: int
    total_plane_routes: int
    total_comparable_routes: int
    avg_train_co2_kg: float
    avg_plane_co2_kg: float
    avg_savings_kg: float
    avg_savings_percent: float

class CountryStatsResponse(BaseModel):
    country_code: str
    nb_stations: int
    nb_airports: int
    nb_train_stations: int
    nb_train_routes: int
    nb_avion_routes: int

class NightVsDayResponse(BaseModel):
    type: str
    nb_routes: int
    avg_distance_km: float
    avg_co2_kg_passenger: float
    total_routes_with_emission: int

# --- Sources & ETL ---

class SourceResponse(BaseModel):
    source_id: int
    source_name: str
    source_type: str
    url: Optional[str]
    version: Optional[str]
    description: Optional[str]
    loaded_at: Optional[str]

class EtlRunResponse(BaseModel):
    run_id: int
    started_at: Optional[str]
    finished_at: Optional[str]
    duration_s: Optional[float]
    mois_traites: Optional[int]
    airports_charges: Optional[int]
    routes_chargees: Optional[int]
    co2_exact: Optional[int]
    co2_fallback: Optional[int]
    erreurs: Optional[int]
    statut: Optional[str]


# ============================================================
# 1. STATIONS
# ============================================================

@router.get(
    "/stations",
    response_model=PaginatedStations,
    tags=["Stations"],
    summary="Liste paginée de toutes les stations",
)
def list_stations(
    page: int = Query(1, ge=1, description="Numéro de page"),
    per_page: int = Query(50, ge=1, le=500, description="Nombre d'éléments par page"),
    country_code: Optional[str] = Query(None, min_length=2, max_length=2, description="Code pays ISO 2 lettres (ex: FR)"),
    is_airport: Optional[bool] = Query(None, description="True = aéroports, False = gares, None = tous"),
    city: Optional[str] = Query(None, description="Filtre exact sur la ville (ILIKE)"),
    search: Optional[str] = Query(None, description="Recherche partielle sur le nom ou la ville"),
    db: Session = Depends(get_db),
):
    conditions = []
    params = {}

    if country_code:
        conditions.append("s.country_code = :country_code")
        params["country_code"] = country_code.upper()
    if is_airport is not None:
        conditions.append("s.is_airport = :is_airport")
        params["is_airport"] = is_airport
    if city:
        conditions.append("s.city ILIKE :city")
        params["city"] = city
    if search:
        conditions.append("(s.station_name ILIKE :search OR s.city ILIKE :search)")
        params["search"] = f"%{search}%"

    where = "WHERE " + " AND ".join(conditions) if conditions else ""

    count_row = db.execute(text(f"SELECT COUNT(*) AS cnt FROM mart.dim_station s {where}"), params).fetchone()
    total = count_row._mapping["cnt"]

    offset = (page - 1) * per_page
    params["limit"] = per_page
    params["offset"] = offset

    rows = db.execute(text(f"""
        SELECT station_id, station_name, city, country_code, latitude, longitude, is_airport, source_id
        FROM mart.dim_station s {where}
        ORDER BY station_name
        LIMIT :limit OFFSET :offset
    """), params).fetchall()

    return PaginatedStations(
        page=page, per_page=per_page, total=total,
        data=[StationResponse(**dict(r._mapping)) for r in rows],
    )


@router.get(
    "/stations/{station_id}",
    response_model=StationResponse,
    tags=["Stations"],
    summary="Détail d'une station par ID",
)
def get_station(station_id: str, db: Session = Depends(get_db)):
    row = db.execute(text("""
        SELECT station_id, station_name, city, country_code, latitude, longitude, is_airport, source_id
        FROM mart.dim_station WHERE station_id = :sid
    """), {"sid": station_id}).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Station introuvable.")
    return StationResponse(**dict(row._mapping))


@router.get(
    "/stations/nearby",
    response_model=List[StationNearbyResponse],
    tags=["Stations"],
    summary="Stations proches d'un point GPS",
)
def stations_nearby(
    lat: float = Query(..., ge=-90, le=90, description="Latitude"),
    lon: float = Query(..., ge=-180, le=180, description="Longitude"),
    radius_km: float = Query(50, ge=1, le=500, description="Rayon de recherche en km"),
    is_airport: Optional[bool] = Query(None, description="Filtrer aéroports ou gares"),
    limit: int = Query(20, ge=1, le=100, description="Nombre max de résultats"),
    db: Session = Depends(get_db),
):
    airport_filter = ""
    if is_airport is not None:
        airport_filter = f"AND is_airport = {'TRUE' if is_airport else 'FALSE'}"

    rows = db.execute(text(f"""
        SELECT *, sub.dist_km FROM (
            SELECT station_id, station_name, city, country_code, latitude, longitude, is_airport, source_id,
                (2 * 6371 * ASIN(SQRT(
                    POWER(SIN(RADIANS((:lat - latitude) / 2)), 2) +
                    COS(RADIANS(:lat)) * COS(RADIANS(latitude)) *
                    POWER(SIN(RADIANS((:lon - longitude) / 2)), 2)
                ))) AS dist_km
            FROM mart.dim_station
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL {airport_filter}
        ) sub
        WHERE sub.dist_km <= :radius
        ORDER BY sub.dist_km ASC
        LIMIT :lim
    """), {"lat": lat, "lon": lon, "radius": radius_km, "lim": limit}).fetchall()

    return [
        StationNearbyResponse(
            station_id=r._mapping["station_id"],
            station_name=r._mapping["station_name"],
            city=r._mapping["city"],
            country_code=r._mapping["country_code"],
            latitude=float(r._mapping["latitude"]) if r._mapping["latitude"] else None,
            longitude=float(r._mapping["longitude"]) if r._mapping["longitude"] else None,
            is_airport=r._mapping["is_airport"],
            source_id=r._mapping["source_id"],
            distance_km=round(float(r._mapping["dist_km"]), 2),
        )
        for r in rows
    ]


# ============================================================
# 2. ROUTES TRAIN
# ============================================================

@router.get(
    "/routes/train",
    response_model=PaginatedRoutesTrain,
    tags=["Routes Train"],
    summary="Liste paginée des routes ferroviaires",
)
def list_routes_train(
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=500),
    dep_city: Optional[str] = Query(None, description="Ville de départ (ILIKE)"),
    arr_city: Optional[str] = Query(None, description="Ville d'arrivée (ILIKE)"),
    is_night_train: Optional[bool] = Query(None, description="True = trains de nuit uniquement"),
    country_code: Optional[str] = Query(None, min_length=2, max_length=2, description="Code pays (départ)"),
    min_distance_km: Optional[float] = Query(None, ge=0, description="Distance minimale en km"),
    db: Session = Depends(get_db),
):
    conditions = []
    params = {}

    if dep_city:
        conditions.append("sd.city ILIKE :dep_city")
        params["dep_city"] = dep_city
    if arr_city:
        conditions.append("sa.city ILIKE :arr_city")
        params["arr_city"] = arr_city
    if is_night_train is not None:
        conditions.append("rt.is_night_train = :night")
        params["night"] = is_night_train
    if country_code:
        conditions.append("sd.country_code = :cc")
        params["cc"] = country_code.upper()
    if min_distance_km is not None:
        conditions.append("rt.distance_km >= :min_dist")
        params["min_dist"] = min_distance_km

    where = "WHERE " + " AND ".join(conditions) if conditions else ""

    count_row = db.execute(text(f"""
        SELECT COUNT(*) AS cnt
        FROM mart.dim_route_train rt
        JOIN mart.dim_station sd ON rt.dep_station_id = sd.station_id
        JOIN mart.dim_station sa ON rt.arr_station_id = sa.station_id
        {where}
    """), params).fetchone()
    total = count_row._mapping["cnt"]

    offset = (page - 1) * per_page
    params["limit"] = per_page
    params["offset"] = offset

    rows = db.execute(text(f"""
        SELECT rt.route_train_id, rt.dep_station_id, sd.station_name AS dep_station_name, sd.city AS dep_city,
               rt.arr_station_id, sa.station_name AS arr_station_name, sa.city AS arr_city,
               rt.distance_km, rt.is_night_train
        FROM mart.dim_route_train rt
        JOIN mart.dim_station sd ON rt.dep_station_id = sd.station_id
        JOIN mart.dim_station sa ON rt.arr_station_id = sa.station_id
        {where}
        ORDER BY rt.route_train_id
        LIMIT :limit OFFSET :offset
    """), params).fetchall()

    return PaginatedRoutesTrain(
        page=page, per_page=per_page, total=total,
        data=[
            RouteTrainResponse(
                route_train_id=r._mapping["route_train_id"],
                dep_station_id=r._mapping["dep_station_id"],
                dep_station_name=r._mapping["dep_station_name"],
                dep_city=r._mapping["dep_city"],
                arr_station_id=r._mapping["arr_station_id"],
                arr_station_name=r._mapping["arr_station_name"],
                arr_city=r._mapping["arr_city"],
                distance_km=float(r._mapping["distance_km"]) if r._mapping["distance_km"] else None,
                is_night_train=r._mapping["is_night_train"],
            )
            for r in rows
        ],
    )


@router.get(
    "/routes/train/{route_train_id}",
    response_model=RouteTrainResponse,
    tags=["Routes Train"],
    summary="Détail d'une route ferroviaire",
)
def get_route_train(route_train_id: int, db: Session = Depends(get_db)):
    row = db.execute(text("""
        SELECT rt.route_train_id, rt.dep_station_id, sd.station_name AS dep_station_name, sd.city AS dep_city,
               rt.arr_station_id, sa.station_name AS arr_station_name, sa.city AS arr_city,
               rt.distance_km, rt.is_night_train
        FROM mart.dim_route_train rt
        JOIN mart.dim_station sd ON rt.dep_station_id = sd.station_id
        JOIN mart.dim_station sa ON rt.arr_station_id = sa.station_id
        WHERE rt.route_train_id = :rid
    """), {"rid": route_train_id}).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Route ferroviaire introuvable.")
    m = row._mapping
    return RouteTrainResponse(
        route_train_id=m["route_train_id"],
        dep_station_id=m["dep_station_id"], dep_station_name=m["dep_station_name"], dep_city=m["dep_city"],
        arr_station_id=m["arr_station_id"], arr_station_name=m["arr_station_name"], arr_city=m["arr_city"],
        distance_km=float(m["distance_km"]) if m["distance_km"] else None,
        is_night_train=m["is_night_train"],
    )


# ============================================================
# 3. ROUTES AVION
# ============================================================

@router.get(
    "/routes/avion",
    response_model=PaginatedRoutesAvion,
    tags=["Routes Avion"],
    summary="Liste paginée des routes aériennes",
)
def list_routes_avion(
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=500),
    dep_city: Optional[str] = Query(None, description="Ville de départ (ILIKE)"),
    arr_city: Optional[str] = Query(None, description="Ville d'arrivée (ILIKE)"),
    country_code: Optional[str] = Query(None, min_length=2, max_length=2, description="Code pays (départ)"),
    min_distance_km: Optional[float] = Query(None, ge=0, description="Distance minimale en km"),
    db: Session = Depends(get_db),
):
    conditions = []
    params = {}

    if dep_city:
        conditions.append("sd.city ILIKE :dep_city")
        params["dep_city"] = dep_city
    if arr_city:
        conditions.append("sa.city ILIKE :arr_city")
        params["arr_city"] = arr_city
    if country_code:
        conditions.append("sd.country_code = :cc")
        params["cc"] = country_code.upper()
    if min_distance_km is not None:
        conditions.append("ra.distance_km >= :min_dist")
        params["min_dist"] = min_distance_km

    where = "WHERE " + " AND ".join(conditions) if conditions else ""

    count_row = db.execute(text(f"""
        SELECT COUNT(*) AS cnt
        FROM mart.dim_route_avion ra
        JOIN mart.dim_station sd ON ra.dep_station_id = sd.station_id
        JOIN mart.dim_station sa ON ra.arr_station_id = sa.station_id
        {where}
    """), params).fetchone()
    total = count_row._mapping["cnt"]

    offset = (page - 1) * per_page
    params["limit"] = per_page
    params["offset"] = offset

    rows = db.execute(text(f"""
        SELECT ra.route_avion_id, ra.dep_station_id, sd.station_name AS dep_station_name, sd.city AS dep_city,
               ra.arr_station_id, sa.station_name AS arr_station_name, sa.city AS arr_city,
               ra.distance_km, ra.dominant_typecode, ra.co2_total_kg
        FROM mart.dim_route_avion ra
        JOIN mart.dim_station sd ON ra.dep_station_id = sd.station_id
        JOIN mart.dim_station sa ON ra.arr_station_id = sa.station_id
        {where}
        ORDER BY ra.route_avion_id
        LIMIT :limit OFFSET :offset
    """), params).fetchall()

    return PaginatedRoutesAvion(
        page=page, per_page=per_page, total=total,
        data=[
            RouteAvionResponse(
                route_avion_id=r._mapping["route_avion_id"],
                dep_station_id=r._mapping["dep_station_id"],
                dep_station_name=r._mapping["dep_station_name"],
                dep_city=r._mapping["dep_city"],
                arr_station_id=r._mapping["arr_station_id"],
                arr_station_name=r._mapping["arr_station_name"],
                arr_city=r._mapping["arr_city"],
                distance_km=float(r._mapping["distance_km"]) if r._mapping["distance_km"] else None,
                dominant_typecode=r._mapping["dominant_typecode"],
                co2_total_kg=float(r._mapping["co2_total_kg"]) if r._mapping["co2_total_kg"] else None,
            )
            for r in rows
        ],
    )


@router.get(
    "/routes/avion/{route_avion_id}",
    response_model=RouteAvionResponse,
    tags=["Routes Avion"],
    summary="Détail d'une route aérienne",
)
def get_route_avion(route_avion_id: int, db: Session = Depends(get_db)):
    row = db.execute(text("""
        SELECT ra.route_avion_id, ra.dep_station_id, sd.station_name AS dep_station_name, sd.city AS dep_city,
               ra.arr_station_id, sa.station_name AS arr_station_name, sa.city AS arr_city,
               ra.distance_km, ra.dominant_typecode, ra.co2_total_kg
        FROM mart.dim_route_avion ra
        JOIN mart.dim_station sd ON ra.dep_station_id = sd.station_id
        JOIN mart.dim_station sa ON ra.arr_station_id = sa.station_id
        WHERE ra.route_avion_id = :rid
    """), {"rid": route_avion_id}).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Route aerienne introuvable.")
    m = row._mapping
    return RouteAvionResponse(
        route_avion_id=m["route_avion_id"],
        dep_station_id=m["dep_station_id"], dep_station_name=m["dep_station_name"], dep_city=m["dep_city"],
        arr_station_id=m["arr_station_id"], arr_station_name=m["arr_station_name"], arr_city=m["arr_city"],
        distance_km=float(m["distance_km"]) if m["distance_km"] else None,
        dominant_typecode=m["dominant_typecode"],
        co2_total_kg=float(m["co2_total_kg"]) if m["co2_total_kg"] else None,
    )


# ============================================================
# 4. VEHICLES
# ============================================================

@router.get(
    "/vehicles/train",
    response_model=List[VehicleTrainResponse],
    tags=["Vehicules"],
    summary="Liste des types de trains",
)
def list_vehicles_train(
    service_type: Optional[str] = Query(None, description="Filtre par type de service"),
    db: Session = Depends(get_db),
):
    condition = "WHERE service_type ILIKE :st" if service_type else ""
    params = {"st": service_type} if service_type else {}
    rows = db.execute(text(f"""
        SELECT vehicle_train_id, label, co2_per_km, service_type
        FROM mart.dim_vehicle_train {condition}
        ORDER BY label
    """), params).fetchall()
    return [
        VehicleTrainResponse(
            vehicle_train_id=r._mapping["vehicle_train_id"],
            label=r._mapping["label"],
            co2_per_km=float(r._mapping["co2_per_km"]),
            service_type=r._mapping["service_type"],
        )
        for r in rows
    ]


@router.get(
    "/vehicles/avion",
    response_model=List[VehicleAvionResponse],
    tags=["Vehicules"],
    summary="Liste des types d'avions",
)
def list_vehicles_avion(
    service_type: Optional[str] = Query(None, description="Filtre par type de service (regional, court_moyen_courrier, long_courrier)"),
    manufacturer: Optional[str] = Query(None, description="Filtre par constructeur (ILIKE)"),
    icao_typecode: Optional[str] = Query(None, description="Filtre par code ICAO type (ex: A20N)"),
    db: Session = Depends(get_db),
):
    conditions = []
    params = {}
    if service_type:
        conditions.append("service_type ILIKE :st")
        params["st"] = service_type
    if manufacturer:
        conditions.append("manufacturer ILIKE :mfr")
        params["mfr"] = f"%{manufacturer}%"
    if icao_typecode:
        conditions.append("icao_typecode = :icao")
        params["icao"] = icao_typecode.upper()

    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    rows = db.execute(text(f"""
        SELECT vehicle_avion_id, label, co2_per_km, service_type, icao_typecode, mtom_kg, num_engines, manufacturer
        FROM mart.dim_vehicle_avion {where}
        ORDER BY label
    """), params).fetchall()
    return [
        VehicleAvionResponse(
            vehicle_avion_id=r._mapping["vehicle_avion_id"],
            label=r._mapping["label"],
            co2_per_km=float(r._mapping["co2_per_km"]),
            service_type=r._mapping["service_type"],
            icao_typecode=r._mapping["icao_typecode"],
            mtom_kg=r._mapping["mtom_kg"],
            num_engines=r._mapping["num_engines"],
            manufacturer=r._mapping["manufacturer"],
        )
        for r in rows
    ]


@router.get(
    "/vehicles/avion/{vehicle_avion_id}",
    response_model=VehicleAvionResponse,
    tags=["Vehicules"],
    summary="Detail d'un type d'avion (EASA CO2, MTOM, moteurs...)",
)
def get_vehicle_avion(vehicle_avion_id: int, db: Session = Depends(get_db)):
    row = db.execute(text("""
        SELECT vehicle_avion_id, label, co2_per_km, service_type, icao_typecode, mtom_kg, num_engines, manufacturer
        FROM mart.dim_vehicle_avion WHERE vehicle_avion_id = :vid
    """), {"vid": vehicle_avion_id}).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Type d'avion introuvable.")
    m = row._mapping
    return VehicleAvionResponse(
        vehicle_avion_id=m["vehicle_avion_id"], label=m["label"],
        co2_per_km=float(m["co2_per_km"]), service_type=m["service_type"],
        icao_typecode=m["icao_typecode"], mtom_kg=m["mtom_kg"],
        num_engines=m["num_engines"], manufacturer=m["manufacturer"],
    )


# ============================================================
# 5. EMISSIONS
# ============================================================

@router.get(
    "/emissions",
    response_model=PaginatedEmissions,
    tags=["Emissions"],
    summary="Liste paginee des faits d'emission CO2",
)
def list_emissions(
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=500),
    transport_mode: Optional[str] = Query(None, description="'train' ou 'avion'"),
    dep_city: Optional[str] = Query(None, description="Ville de depart (ILIKE)"),
    arr_city: Optional[str] = Query(None, description="Ville d'arrivee (ILIKE)"),
    db: Session = Depends(get_db),
):
    conditions = []
    params = {}
    if transport_mode:
        conditions.append("fe.transport_mode = :tm")
        params["tm"] = transport_mode
    if dep_city:
        conditions.append("sd.city ILIKE :dep_city")
        params["dep_city"] = dep_city
    if arr_city:
        conditions.append("sa.city ILIKE :arr_city")
        params["arr_city"] = arr_city

    where = "WHERE " + " AND ".join(conditions) if conditions else ""

    count_row = db.execute(text(f"""
        SELECT COUNT(*) AS cnt
        FROM mart.fact_emission fe
        LEFT JOIN mart.dim_route_train rt ON fe.route_train_id = rt.route_train_id
        LEFT JOIN mart.dim_route_avion ra ON fe.route_avion_id = ra.route_avion_id
        LEFT JOIN mart.dim_station sd ON COALESCE(rt.dep_station_id, ra.dep_station_id) = sd.station_id
        LEFT JOIN mart.dim_station sa ON COALESCE(rt.arr_station_id, ra.arr_station_id) = sa.station_id
        {where}
    """), params).fetchone()
    total = count_row._mapping["cnt"]

    offset = (page - 1) * per_page
    params["limit"] = per_page
    params["offset"] = offset

    rows = db.execute(text(f"""
        SELECT fe.fact_id, fe.transport_mode, fe.route_train_id, fe.vehicle_train_id,
               fe.route_avion_id, fe.vehicle_avion_id, fe.co2_kg_passenger,
               sd.city AS dep_city, sa.city AS arr_city
        FROM mart.fact_emission fe
        LEFT JOIN mart.dim_route_train rt ON fe.route_train_id = rt.route_train_id
        LEFT JOIN mart.dim_route_avion ra ON fe.route_avion_id = ra.route_avion_id
        LEFT JOIN mart.dim_station sd ON COALESCE(rt.dep_station_id, ra.dep_station_id) = sd.station_id
        LEFT JOIN mart.dim_station sa ON COALESCE(rt.arr_station_id, ra.arr_station_id) = sa.station_id
        {where}
        ORDER BY fe.fact_id
        LIMIT :limit OFFSET :offset
    """), params).fetchall()

    return PaginatedEmissions(
        page=page, per_page=per_page, total=total,
        data=[
            EmissionResponse(
                fact_id=r._mapping["fact_id"],
                transport_mode=r._mapping["transport_mode"],
                route_train_id=r._mapping["route_train_id"],
                vehicle_train_id=r._mapping["vehicle_train_id"],
                route_avion_id=r._mapping["route_avion_id"],
                vehicle_avion_id=r._mapping["vehicle_avion_id"],
                co2_kg_passenger=float(r._mapping["co2_kg_passenger"]),
                dep_city=r._mapping["dep_city"],
                arr_city=r._mapping["arr_city"],
            )
            for r in rows
        ],
    )


@router.get(
    "/emissions/{fact_id}",
    response_model=EmissionResponse,
    tags=["Emissions"],
    summary="Detail d'un fait d'emission",
)
def get_emission(fact_id: int, db: Session = Depends(get_db)):
    row = db.execute(text("""
        SELECT fe.fact_id, fe.transport_mode, fe.route_train_id, fe.vehicle_train_id,
               fe.route_avion_id, fe.vehicle_avion_id, fe.co2_kg_passenger,
               sd.city AS dep_city, sa.city AS arr_city
        FROM mart.fact_emission fe
        LEFT JOIN mart.dim_route_train rt ON fe.route_train_id = rt.route_train_id
        LEFT JOIN mart.dim_route_avion ra ON fe.route_avion_id = ra.route_avion_id
        LEFT JOIN mart.dim_station sd ON COALESCE(rt.dep_station_id, ra.dep_station_id) = sd.station_id
        LEFT JOIN mart.dim_station sa ON COALESCE(rt.arr_station_id, ra.arr_station_id) = sa.station_id
        WHERE fe.fact_id = :fid
    """), {"fid": fact_id}).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Emission introuvable.")
    m = row._mapping
    return EmissionResponse(
        fact_id=m["fact_id"], transport_mode=m["transport_mode"],
        route_train_id=m["route_train_id"], vehicle_train_id=m["vehicle_train_id"],
        route_avion_id=m["route_avion_id"], vehicle_avion_id=m["vehicle_avion_id"],
        co2_kg_passenger=float(m["co2_kg_passenger"]),
        dep_city=m["dep_city"], arr_city=m["arr_city"],
    )


# ============================================================
# 6. COMPARAISON TRAIN VS AVION
# ============================================================

@router.get(
    "/compare/cities",
    response_model=List[CompareResponse],
    tags=["Comparaison"],
    summary="Compare CO2 train vs avion pour un trajet ville a ville",
)
def compare_cities(
    dep_city: str = Query(..., description="Ville de depart (ex: Paris)"),
    arr_city: str = Query(..., description="Ville d'arrivee (ex: Lyon)"),
    train_type: Optional[str] = Query(None, description="Type de train : 'jour', 'nuit' ou None pour tous"),
    min_distance_km: Optional[float] = Query(None, ge=0, description="Distance minimale en km"),
    db: Session = Depends(get_db),
):
    night_filter = ""
    if train_type == "nuit":
        night_filter = "AND rt.is_night_train = TRUE"
    elif train_type == "jour":
        night_filter = "AND rt.is_night_train = FALSE"

    dist_filter_train = ""
    dist_filter_avion = ""
    params = {"dep_city": dep_city, "arr_city": arr_city}
    if min_distance_km is not None:
        dist_filter_train = "AND rt.distance_km >= :min_dist"
        dist_filter_avion = "AND ra.distance_km >= :min_dist"
        params["min_dist"] = min_distance_km

    query = text(f"""
        SELECT 'train' as mode, st_dep.city as dep_city, st_arr.city as arr_city,
               rt.distance_km, vt.co2_per_km, fe.co2_kg_passenger
        FROM mart.fact_emission fe
        JOIN mart.dim_route_train rt ON fe.route_train_id = rt.route_train_id
        JOIN mart.dim_vehicle_train vt ON fe.vehicle_train_id = vt.vehicle_train_id
        JOIN mart.dim_station st_dep ON rt.dep_station_id = st_dep.station_id
        JOIN mart.dim_station st_arr ON rt.arr_station_id = st_arr.station_id
        WHERE st_dep.city ILIKE :dep_city AND st_arr.city ILIKE :arr_city
          {night_filter} {dist_filter_train}

        UNION ALL

        SELECT 'avion' as mode, st_dep.city as dep_city, st_arr.city as arr_city,
               ra.distance_km, va.co2_per_km, fe.co2_kg_passenger
        FROM mart.fact_emission fe
        JOIN mart.dim_route_avion ra ON fe.route_avion_id = ra.route_avion_id
        JOIN mart.dim_vehicle_avion va ON fe.vehicle_avion_id = va.vehicle_avion_id
        JOIN mart.dim_station st_dep ON ra.dep_station_id = st_dep.station_id
        JOIN mart.dim_station st_arr ON ra.arr_station_id = st_arr.station_id
        WHERE st_dep.city ILIKE :dep_city AND st_arr.city ILIKE :arr_city
          {dist_filter_avion}
    """)

    result = db.execute(query, params).fetchall()
    if not result:
        raise HTTPException(status_code=404, detail="Aucun trajet trouve pour ces villes.")

    return [
        CompareResponse(
            mode=r._mapping["mode"], dep_city=r._mapping["dep_city"], arr_city=r._mapping["arr_city"],
            distance_km=float(r._mapping["distance_km"] or 0),
            co2_per_km=float(r._mapping["co2_per_km"] or 0),
            co2_kg_passenger=float(r._mapping["co2_kg_passenger"] or 0),
        )
        for r in result
    ]


@router.get(
    "/compare/winner",
    response_model=WinnerResponse,
    tags=["Comparaison"],
    summary="Mode le plus ecologique pour un trajet",
)
def compare_winner(
    dep_city: str = Query(..., description="Ville de depart"),
    arr_city: str = Query(..., description="Ville d'arrivee"),
    db: Session = Depends(get_db),
):
    query = text("""
        SELECT
            AVG(CASE WHEN fe.transport_mode = 'train' THEN fe.co2_kg_passenger END) AS train_co2,
            AVG(CASE WHEN fe.transport_mode = 'avion' THEN fe.co2_kg_passenger END) AS plane_co2,
            MAX(CASE WHEN fe.transport_mode = 'train' THEN st_dep.city END) AS dep_city,
            MAX(CASE WHEN fe.transport_mode = 'train' THEN st_arr.city END) AS arr_city
        FROM mart.fact_emission fe
        LEFT JOIN mart.dim_route_train rt ON fe.route_train_id = rt.route_train_id
        LEFT JOIN mart.dim_route_avion ra ON fe.route_avion_id = ra.route_avion_id
        LEFT JOIN mart.dim_station st_dep ON COALESCE(rt.dep_station_id, ra.dep_station_id) = st_dep.station_id
        LEFT JOIN mart.dim_station st_arr ON COALESCE(rt.arr_station_id, ra.arr_station_id) = st_arr.station_id
        WHERE st_dep.city ILIKE :dep_city AND st_arr.city ILIKE :arr_city
    """)

    row = db.execute(query, {"dep_city": dep_city, "arr_city": arr_city}).fetchone()
    if not row or (row._mapping["train_co2"] is None and row._mapping["plane_co2"] is None):
        raise HTTPException(status_code=404, detail="Aucun trajet comparable trouve pour ces villes.")

    m = row._mapping
    train_co2 = float(m["train_co2"]) if m["train_co2"] is not None else None
    plane_co2 = float(m["plane_co2"]) if m["plane_co2"] is not None else None

    if train_co2 is not None and plane_co2 is not None:
        if train_co2 <= plane_co2:
            greener = "train"
            savings_kg = round(plane_co2 - train_co2, 3)
            savings_pct = round((savings_kg / plane_co2) * 100, 1) if plane_co2 > 0 else 0.0
        else:
            greener = "avion"
            savings_kg = round(train_co2 - plane_co2, 3)
            savings_pct = round((savings_kg / train_co2) * 100, 1) if train_co2 > 0 else 0.0
    elif train_co2 is not None:
        greener, savings_kg, savings_pct = "train", None, None
    else:
        greener, savings_kg, savings_pct = "avion", None, None

    return WinnerResponse(
        greener_mode=greener,
        dep_city=m["dep_city"] or dep_city, arr_city=m["arr_city"] or arr_city,
        train_co2_kg=train_co2, plane_co2_kg=plane_co2,
        savings_kg=savings_kg, savings_percent=savings_pct,
    )


@router.get(
    "/compare/passengers",
    response_model=List[PassengersResponse],
    tags=["Comparaison"],
    summary="CO2 total pour N passagers avec equivalences",
)
def compare_passengers(
    dep_city: str = Query(..., description="Ville de depart"),
    arr_city: str = Query(..., description="Ville d'arrivee"),
    passengers: int = Query(..., ge=1, le=500, description="Nombre de passagers"),
    db: Session = Depends(get_db),
):
    """
    Equivalences fournies :
    - equivalent_trees : arbres necessaires pour absorber ce CO2 sur 1 an (1 arbre ~ 22 kg CO2/an)
    - equivalent_car_km : kilometres en voiture equivalents (voiture moyenne ~ 0.21 kg CO2/km)
    """
    query = text("""
        SELECT fe.transport_mode as mode, st_dep.city as dep_city, st_arr.city as arr_city,
               AVG(fe.co2_kg_passenger) as co2_per_passenger
        FROM mart.fact_emission fe
        LEFT JOIN mart.dim_route_train rt ON fe.route_train_id = rt.route_train_id
        LEFT JOIN mart.dim_route_avion ra ON fe.route_avion_id = ra.route_avion_id
        LEFT JOIN mart.dim_station st_dep ON COALESCE(rt.dep_station_id, ra.dep_station_id) = st_dep.station_id
        LEFT JOIN mart.dim_station st_arr ON COALESCE(rt.arr_station_id, ra.arr_station_id) = st_arr.station_id
        WHERE st_dep.city ILIKE :dep_city AND st_arr.city ILIKE :arr_city
        GROUP BY fe.transport_mode, st_dep.city, st_arr.city
        ORDER BY fe.transport_mode
    """)

    result = db.execute(query, {"dep_city": dep_city, "arr_city": arr_city}).fetchall()
    if not result:
        raise HTTPException(status_code=404, detail="Aucun trajet trouve pour ces villes.")

    CO2_PER_TREE_KG_YEAR = 22.0
    CO2_PER_CAR_KM = 0.21

    return [
        PassengersResponse(
            mode=r._mapping["mode"], dep_city=r._mapping["dep_city"], arr_city=r._mapping["arr_city"],
            passengers=passengers,
            total_co2_kg=round(float(r._mapping["co2_per_passenger"] or 0) * passengers, 3),
            co2_per_passenger_kg=round(float(r._mapping["co2_per_passenger"] or 0), 3),
            equivalent_trees=round(float(r._mapping["co2_per_passenger"] or 0) * passengers / CO2_PER_TREE_KG_YEAR, 1),
            equivalent_car_km=round(float(r._mapping["co2_per_passenger"] or 0) * passengers / CO2_PER_CAR_KM, 1),
        )
        for r in result
    ]


# ============================================================
# 7. RECHERCHE & EXPLORATION
# ============================================================

@router.get(
    "/search/cities",
    response_model=List[CitySearchResponse],
    tags=["Recherche"],
    summary="Autocomplete / recherche de villes disponibles",
)
def search_cities(
    q: str = Query(..., min_length=2, description="Terme de recherche (ex: Par → Paris, Parma...)"),
    limit: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    rows = db.execute(text("""
        SELECT city, country_code,
               BOOL_OR(NOT is_airport) AS has_train,
               BOOL_OR(is_airport) AS has_airport
        FROM mart.dim_station
        WHERE city ILIKE :q AND city IS NOT NULL AND city != ''
        GROUP BY city, country_code
        ORDER BY city
        LIMIT :lim
    """), {"q": f"{q}%", "lim": limit}).fetchall()

    return [
        CitySearchResponse(
            city=r._mapping["city"], country_code=r._mapping["country_code"],
            has_train=r._mapping["has_train"], has_airport=r._mapping["has_airport"],
        )
        for r in rows
    ]


@router.get(
    "/search/departures",
    response_model=List[DepartureResponse],
    tags=["Recherche"],
    summary="Toutes les destinations depuis une ville avec CO2 par mode",
)
def search_departures(
    dep_city: str = Query(..., description="Ville de depart"),
    train_type: Optional[str] = Query(None, description="'jour', 'nuit' ou None"),
    min_distance_km: Optional[float] = Query(None, ge=0),
    db: Session = Depends(get_db),
):
    night_filter = ""
    if train_type == "nuit":
        night_filter = "AND rt.is_night_train = TRUE"
    elif train_type == "jour":
        night_filter = "AND rt.is_night_train = FALSE"

    dist_filter = ""
    dist_filter_avion = ""
    params = {"dep_city": dep_city}
    if min_distance_km is not None:
        dist_filter = "AND rt.distance_km >= :min_dist"
        dist_filter_avion = "AND ra.distance_km >= :min_dist"
        params["min_dist"] = min_distance_km

    query = text(f"""
        WITH train_routes AS (
            SELECT st_arr.city AS arr_city, rt.distance_km, AVG(fe.co2_kg_passenger) AS train_co2
            FROM mart.fact_emission fe
            JOIN mart.dim_route_train rt ON fe.route_train_id = rt.route_train_id
            JOIN mart.dim_station st_dep ON rt.dep_station_id = st_dep.station_id
            JOIN mart.dim_station st_arr ON rt.arr_station_id = st_arr.station_id
            WHERE fe.transport_mode = 'train' AND st_dep.city ILIKE :dep_city
              {night_filter} {dist_filter}
            GROUP BY st_arr.city, rt.distance_km
        ),
        plane_routes AS (
            SELECT st_arr.city AS arr_city, ra.distance_km, AVG(fe.co2_kg_passenger) AS plane_co2
            FROM mart.fact_emission fe
            JOIN mart.dim_route_avion ra ON fe.route_avion_id = ra.route_avion_id
            JOIN mart.dim_station st_dep ON ra.dep_station_id = st_dep.station_id
            JOIN mart.dim_station st_arr ON ra.arr_station_id = st_arr.station_id
            WHERE fe.transport_mode = 'avion' AND st_dep.city ILIKE :dep_city
              {dist_filter_avion}
            GROUP BY st_arr.city, ra.distance_km
        )
        SELECT COALESCE(t.arr_city, p.arr_city) AS arr_city,
               COALESCE(t.distance_km, p.distance_km) AS distance_km,
               ROUND(t.train_co2::numeric, 3) AS train_co2_kg,
               ROUND(p.plane_co2::numeric, 3) AS plane_co2_kg,
               CASE
                   WHEN t.train_co2 IS NOT NULL AND p.plane_co2 IS NOT NULL THEN
                       CASE WHEN t.train_co2 <= p.plane_co2 THEN 'train' ELSE 'avion' END
                   WHEN t.train_co2 IS NOT NULL THEN 'train'
                   WHEN p.plane_co2 IS NOT NULL THEN 'avion'
               END AS greener_mode
        FROM train_routes t
        FULL OUTER JOIN plane_routes p ON t.arr_city = p.arr_city
        ORDER BY arr_city
    """)

    result = db.execute(query, params).fetchall()
    if not result:
        raise HTTPException(status_code=404, detail=f"Aucune destination trouvee depuis {dep_city}.")

    return [
        DepartureResponse(
            arr_city=r._mapping["arr_city"],
            distance_km=float(r._mapping["distance_km"]) if r._mapping["distance_km"] else None,
            train_co2_kg=float(r._mapping["train_co2_kg"]) if r._mapping["train_co2_kg"] else None,
            plane_co2_kg=float(r._mapping["plane_co2_kg"]) if r._mapping["plane_co2_kg"] else None,
            greener_mode=r._mapping["greener_mode"],
        )
        for r in result
    ]


@router.get(
    "/locations/common-cities",
    tags=["Recherche"],
    summary="Villes desservies par train ET avion",
)
def get_common_cities(db: Session = Depends(get_db)):
    query = text("""
        SELECT DISTINCT g.city
        FROM mart.dim_station g
        JOIN mart.dim_station a ON a.is_airport = TRUE
        WHERE g.is_airport = FALSE
          AND g.city IS NOT NULL AND g.city != ''
          AND g.latitude IS NOT NULL AND g.longitude IS NOT NULL
          AND a.latitude IS NOT NULL AND a.longitude IS NOT NULL
          AND (
            2 * 6371 * ASIN(SQRT(
              POWER(SIN(RADIANS(a.latitude - g.latitude) / 2), 2) +
              COS(RADIANS(g.latitude)) * COS(RADIANS(a.latitude)) *
              POWER(SIN(RADIANS(a.longitude - g.longitude) / 2), 2)
            ))
          ) < 80
        ORDER BY g.city
    """)
    result = db.execute(query).fetchall()
    return {"common_cities": [row._mapping["city"] for row in result]}


# ============================================================
# 8. CLASSEMENTS & STATISTIQUES
# ============================================================

@router.get(
    "/ranking/greener-routes",
    response_model=List[RouteRankingResponse],
    tags=["Classements & Stats"],
    summary="Top N routes ou le train economise le plus de CO2",
)
def ranking_greener_routes(
    limit: int = Query(10, ge=1, le=100),
    db: Session = Depends(get_db),
):
    query = text("""
        WITH train_data AS (
            SELECT st_dep.city AS dep_city, st_arr.city AS arr_city, AVG(fe.co2_kg_passenger) AS train_co2
            FROM mart.fact_emission fe
            JOIN mart.dim_route_train rt ON fe.route_train_id = rt.route_train_id
            JOIN mart.dim_station st_dep ON rt.dep_station_id = st_dep.station_id
            JOIN mart.dim_station st_arr ON rt.arr_station_id = st_arr.station_id
            WHERE fe.transport_mode = 'train'
            GROUP BY st_dep.city, st_arr.city
        ),
        plane_data AS (
            SELECT st_dep.city AS dep_city, st_arr.city AS arr_city, AVG(fe.co2_kg_passenger) AS plane_co2
            FROM mart.fact_emission fe
            JOIN mart.dim_route_avion ra ON fe.route_avion_id = ra.route_avion_id
            JOIN mart.dim_station st_dep ON ra.dep_station_id = st_dep.station_id
            JOIN mart.dim_station st_arr ON ra.arr_station_id = st_arr.station_id
            WHERE fe.transport_mode = 'avion'
            GROUP BY st_dep.city, st_arr.city
        )
        SELECT t.dep_city, t.arr_city,
               ROUND(t.train_co2::numeric, 3) AS train_co2_kg,
               ROUND(p.plane_co2::numeric, 3) AS plane_co2_kg,
               ROUND((p.plane_co2 - t.train_co2)::numeric, 3) AS savings_kg,
               ROUND(((p.plane_co2 - t.train_co2) / p.plane_co2 * 100)::numeric, 1) AS savings_percent
        FROM train_data t
        JOIN plane_data p ON t.dep_city = p.dep_city AND t.arr_city = p.arr_city
        WHERE p.plane_co2 > t.train_co2
        ORDER BY savings_kg DESC
        LIMIT :limit
    """)

    result = db.execute(query, {"limit": limit}).fetchall()
    if not result:
        raise HTTPException(status_code=404, detail="Aucune route comparable trouvee.")

    return [
        RouteRankingResponse(
            dep_city=r._mapping["dep_city"], arr_city=r._mapping["arr_city"],
            train_co2_kg=float(r._mapping["train_co2_kg"]),
            plane_co2_kg=float(r._mapping["plane_co2_kg"]),
            savings_kg=float(r._mapping["savings_kg"]),
            savings_percent=float(r._mapping["savings_percent"]),
        )
        for r in result
    ]


@router.get(
    "/ranking/longest-routes",
    response_model=List[LongestRouteResponse],
    tags=["Classements & Stats"],
    summary="Routes les plus longues par mode de transport",
)
def ranking_longest_routes(
    transport_mode: Optional[str] = Query(None, description="'train' ou 'avion' (None = tous)"),
    limit: int = Query(10, ge=1, le=100),
    db: Session = Depends(get_db),
):
    parts = []
    params = {"limit": limit}

    if transport_mode is None or transport_mode == "train":
        parts.append("""
            SELECT sd.city AS dep_city, sa.city AS arr_city, rt.distance_km, 'train' AS transport_mode
            FROM mart.dim_route_train rt
            JOIN mart.dim_station sd ON rt.dep_station_id = sd.station_id
            JOIN mart.dim_station sa ON rt.arr_station_id = sa.station_id
            WHERE rt.distance_km IS NOT NULL
        """)
    if transport_mode is None or transport_mode == "avion":
        parts.append("""
            SELECT sd.city AS dep_city, sa.city AS arr_city, ra.distance_km, 'avion' AS transport_mode
            FROM mart.dim_route_avion ra
            JOIN mart.dim_station sd ON ra.dep_station_id = sd.station_id
            JOIN mart.dim_station sa ON ra.arr_station_id = sa.station_id
            WHERE ra.distance_km IS NOT NULL
        """)

    if not parts:
        raise HTTPException(status_code=400, detail="transport_mode doit etre 'train', 'avion' ou vide.")

    union = " UNION ALL ".join(parts)
    rows = db.execute(text(f"""
        SELECT * FROM ({union}) sub ORDER BY distance_km DESC LIMIT :limit
    """), params).fetchall()

    return [
        LongestRouteResponse(
            dep_city=r._mapping["dep_city"], arr_city=r._mapping["arr_city"],
            distance_km=float(r._mapping["distance_km"]),
            transport_mode=r._mapping["transport_mode"],
        )
        for r in rows
    ]


@router.get(
    "/stats/network",
    response_model=NetworkStatsResponse,
    tags=["Classements & Stats"],
    summary="Statistiques globales du reseau",
)
def stats_network(db: Session = Depends(get_db)):
    query = text("""
        WITH train_agg AS (
            SELECT st_dep.city AS dep_city, st_arr.city AS arr_city, AVG(fe.co2_kg_passenger) AS train_co2
            FROM mart.fact_emission fe
            JOIN mart.dim_route_train rt ON fe.route_train_id = rt.route_train_id
            JOIN mart.dim_station st_dep ON rt.dep_station_id = st_dep.station_id
            JOIN mart.dim_station st_arr ON rt.arr_station_id = st_arr.station_id
            WHERE fe.transport_mode = 'train'
            GROUP BY st_dep.city, st_arr.city
        ),
        plane_agg AS (
            SELECT st_dep.city AS dep_city, st_arr.city AS arr_city, AVG(fe.co2_kg_passenger) AS plane_co2
            FROM mart.fact_emission fe
            JOIN mart.dim_route_avion ra ON fe.route_avion_id = ra.route_avion_id
            JOIN mart.dim_station st_dep ON ra.dep_station_id = st_dep.station_id
            JOIN mart.dim_station st_arr ON ra.arr_station_id = st_arr.station_id
            WHERE fe.transport_mode = 'avion'
            GROUP BY st_dep.city, st_arr.city
        ),
        comparable AS (
            SELECT t.train_co2, p.plane_co2, (p.plane_co2 - t.train_co2) AS savings
            FROM train_agg t
            JOIN plane_agg p ON t.dep_city = p.dep_city AND t.arr_city = p.arr_city
        )
        SELECT
            (SELECT COUNT(*) FROM train_agg) AS total_train_routes,
            (SELECT COUNT(*) FROM plane_agg) AS total_plane_routes,
            COUNT(*) AS total_comparable_routes,
            ROUND(AVG(train_co2)::numeric, 3) AS avg_train_co2_kg,
            ROUND(AVG(plane_co2)::numeric, 3) AS avg_plane_co2_kg,
            ROUND(AVG(savings)::numeric, 3) AS avg_savings_kg,
            ROUND((AVG(savings) / NULLIF(AVG(plane_co2), 0) * 100)::numeric, 1) AS avg_savings_percent
        FROM comparable
    """)

    row = db.execute(query).fetchone()
    if not row:
        raise HTTPException(status_code=500, detail="Impossible de calculer les statistiques reseau.")

    m = row._mapping
    return NetworkStatsResponse(
        total_train_routes=int(m["total_train_routes"] or 0),
        total_plane_routes=int(m["total_plane_routes"] or 0),
        total_comparable_routes=int(m["total_comparable_routes"] or 0),
        avg_train_co2_kg=float(m["avg_train_co2_kg"] or 0),
        avg_plane_co2_kg=float(m["avg_plane_co2_kg"] or 0),
        avg_savings_kg=float(m["avg_savings_kg"] or 0),
        avg_savings_percent=float(m["avg_savings_percent"] or 0),
    )


@router.get(
    "/stats/by-country",
    response_model=List[CountryStatsResponse],
    tags=["Classements & Stats"],
    summary="Statistiques agregees par pays",
)
def stats_by_country(
    country_code: Optional[str] = Query(None, min_length=2, max_length=2, description="Filtrer un pays (ex: FR)"),
    db: Session = Depends(get_db),
):
    cc_filter = ""
    params = {}
    if country_code:
        cc_filter = "WHERE s.country_code = :cc"
        params["cc"] = country_code.upper()

    rows = db.execute(text(f"""
        SELECT
            s.country_code,
            COUNT(DISTINCT s.station_id) AS nb_stations,
            COUNT(DISTINCT CASE WHEN s.is_airport THEN s.station_id END) AS nb_airports,
            COUNT(DISTINCT CASE WHEN NOT s.is_airport THEN s.station_id END) AS nb_train_stations,
            COUNT(DISTINCT rt.route_train_id) AS nb_train_routes,
            COUNT(DISTINCT ra.route_avion_id) AS nb_avion_routes
        FROM mart.dim_station s
        LEFT JOIN mart.dim_route_train rt ON s.station_id = rt.dep_station_id
        LEFT JOIN mart.dim_route_avion ra ON s.station_id = ra.dep_station_id
        {cc_filter}
        GROUP BY s.country_code
        ORDER BY nb_stations DESC
    """), params).fetchall()

    return [
        CountryStatsResponse(
            country_code=r._mapping["country_code"],
            nb_stations=int(r._mapping["nb_stations"]),
            nb_airports=int(r._mapping["nb_airports"]),
            nb_train_stations=int(r._mapping["nb_train_stations"]),
            nb_train_routes=int(r._mapping["nb_train_routes"]),
            nb_avion_routes=int(r._mapping["nb_avion_routes"]),
        )
        for r in rows
    ]


@router.get(
    "/stats/night-vs-day",
    response_model=List[NightVsDayResponse],
    tags=["Classements & Stats"],
    summary="Comparaison trains de jour vs trains de nuit",
)
def stats_night_vs_day(db: Session = Depends(get_db)):
    rows = db.execute(text("""
        SELECT
            CASE WHEN rt.is_night_train THEN 'nuit' ELSE 'jour' END AS type,
            COUNT(DISTINCT rt.route_train_id) AS nb_routes,
            ROUND(AVG(rt.distance_km)::numeric, 1) AS avg_distance_km,
            ROUND(AVG(fe.co2_kg_passenger)::numeric, 3) AS avg_co2_kg_passenger,
            COUNT(fe.fact_id) AS total_routes_with_emission
        FROM mart.dim_route_train rt
        LEFT JOIN mart.fact_emission fe ON fe.route_train_id = rt.route_train_id AND fe.transport_mode = 'train'
        GROUP BY rt.is_night_train
        ORDER BY type
    """)).fetchall()

    return [
        NightVsDayResponse(
            type=r._mapping["type"],
            nb_routes=int(r._mapping["nb_routes"]),
            avg_distance_km=float(r._mapping["avg_distance_km"] or 0),
            avg_co2_kg_passenger=float(r._mapping["avg_co2_kg_passenger"] or 0),
            total_routes_with_emission=int(r._mapping["total_routes_with_emission"]),
        )
        for r in rows
    ]


# ============================================================
# 9. SOURCES & ETL
# ============================================================

@router.get(
    "/sources",
    response_model=List[SourceResponse],
    tags=["Sources & ETL"],
    summary="Liste des sources de donnees (tracabilite RGPD)",
)
def list_sources(db: Session = Depends(get_db)):
    rows = db.execute(text("""
        SELECT source_id, source_name, source_type, url, version, description, loaded_at::text AS loaded_at
        FROM mart.dim_source ORDER BY source_name
    """)).fetchall()
    return [SourceResponse(**dict(r._mapping)) for r in rows]


@router.get(
    "/sources/{source_id}",
    response_model=SourceResponse,
    tags=["Sources & ETL"],
    summary="Detail d'une source de donnees",
)
def get_source(source_id: int, db: Session = Depends(get_db)):
    row = db.execute(text("""
        SELECT source_id, source_name, source_type, url, version, description, loaded_at::text AS loaded_at
        FROM mart.dim_source WHERE source_id = :sid
    """), {"sid": source_id}).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Source introuvable.")
    return SourceResponse(**dict(row._mapping))


@router.get(
    "/etl/runs",
    response_model=List[EtlRunResponse],
    tags=["Sources & ETL"],
    summary="Historique des executions ETL",
)
def list_etl_runs(
    limit: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    rows = db.execute(text("""
        SELECT run_id, started_at::text AS started_at, finished_at::text AS finished_at,
               duration_s, mois_traites, airports_charges, routes_chargees,
               co2_exact, co2_fallback, erreurs, statut
        FROM mart.etl_run_log
        ORDER BY started_at DESC
        LIMIT :lim
    """), {"lim": limit}).fetchall()
    return [EtlRunResponse(**dict(r._mapping)) for r in rows]


@router.get(
    "/etl/runs/latest",
    response_model=EtlRunResponse,
    tags=["Sources & ETL"],
    summary="Dernier run ETL (statut, duree, erreurs)",
)
def get_latest_etl_run(db: Session = Depends(get_db)):
    row = db.execute(text("""
        SELECT run_id, started_at::text AS started_at, finished_at::text AS finished_at,
               duration_s, mois_traites, airports_charges, routes_chargees,
               co2_exact, co2_fallback, erreurs, statut
        FROM mart.etl_run_log
        ORDER BY started_at DESC
        LIMIT 1
    """)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Aucun run ETL enregistre.")
    return EtlRunResponse(**dict(row._mapping))


# ============================================================
# 10. HEALTH
# ============================================================

@router.get(
    "/health",
    tags=["Sante"],
    summary="Health check de l'API et de la base de donnees",
)
def health_check(db: Session = Depends(get_db)):
    try:
        db.execute(text("SELECT 1")).fetchone()
        return {"status": "ok", "database": "connected"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Base de donnees inaccessible: {str(e)}")
