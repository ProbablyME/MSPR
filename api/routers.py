from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List, Optional
from pydantic import BaseModel
from database import get_db

router = APIRouter()

# ============================================================
# MODELS
# ============================================================

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
    equivalent_trees: float       # arbres nécessaires pour absorber le CO2 (1 arbre ~ 22 kg CO2/an)
    equivalent_car_km: float      # km en voiture équivalent (0.21 kg CO2/km moyen)

class RouteRankingResponse(BaseModel):
    dep_city: str
    arr_city: str
    train_co2_kg: float
    plane_co2_kg: float
    savings_kg: float
    savings_percent: float

class DepartureResponse(BaseModel):
    arr_city: str
    distance_km: Optional[float]
    train_co2_kg: Optional[float]
    plane_co2_kg: Optional[float]
    greener_mode: Optional[str]

class NetworkStatsResponse(BaseModel):
    total_train_routes: int
    total_plane_routes: int
    total_comparable_routes: int
    avg_train_co2_kg: float
    avg_plane_co2_kg: float
    avg_savings_kg: float
    avg_savings_percent: float


# ============================================================
# EXISTING ENDPOINTS
# ============================================================

@router.get("/compare/cities", response_model=List[CompareResponse], summary="Compare Train vs Plane for a specific origin/destination")
def compare_cities(
    dep_city: str = Query(..., description="Ville de départ (ex: Paris)"),
    arr_city: str = Query(..., description="Ville d'arrivée (ex: Lyon)"),
    train_type: Optional[str] = Query(None, description="Type de train : 'jour', 'nuit' ou None pour tous"),
    min_distance_km: Optional[float] = Query(None, ge=0, description="Distance minimale en km (ex: 100 pour filtrer les trajets courts)"),
    db: Session = Depends(get_db)
):
    """
    Compare les émissions de CO2 entre les trains et les avions pour le même trajet.

    - **train_type** : filtre sur 'jour' (trains de jour GTFS) ou 'nuit' (trains de nuit Back on Track)
    - **min_distance_km** : exclut les trajets trop courts pour être comparés à l'avion (recommandé : 100)
    """
    night_filter = ""
    if train_type == "nuit":
        night_filter = "AND rt.is_night_train = TRUE"
    elif train_type == "jour":
        night_filter = "AND rt.is_night_train = FALSE"

    dist_filter_train = f"AND rt.distance_km >= {min_distance_km}" if min_distance_km else ""
    dist_filter_avion = f"AND ra.distance_km >= {min_distance_km}" if min_distance_km else ""

    query = text(f"""
        SELECT
            'train' as mode,
            st_dep.city as dep_city,
            st_arr.city as arr_city,
            rt.distance_km,
            vt.co2_per_km,
            fe.co2_kg_passenger
        FROM mart.fact_emission fe
        JOIN mart.dim_route_train rt ON fe.route_train_id = rt.route_train_id
        JOIN mart.dim_vehicle_train vt ON fe.vehicle_train_id = vt.vehicle_train_id
        JOIN mart.dim_station st_dep ON rt.dep_station_id = st_dep.station_id
        JOIN mart.dim_station st_arr ON rt.arr_station_id = st_arr.station_id
        WHERE st_dep.city ILIKE :dep_city AND st_arr.city ILIKE :arr_city
          {night_filter} {dist_filter_train}

        UNION ALL

        SELECT
            'avion' as mode,
            st_dep.city as dep_city,
            st_arr.city as arr_city,
            ra.distance_km,
            va.co2_per_km,
            fe.co2_kg_passenger
        FROM mart.fact_emission fe
        JOIN mart.dim_route_avion ra ON fe.route_avion_id = ra.route_avion_id
        JOIN mart.dim_vehicle_avion va ON fe.vehicle_avion_id = va.vehicle_avion_id
        JOIN mart.dim_station st_dep ON ra.dep_station_id = st_dep.station_id
        JOIN mart.dim_station st_arr ON ra.arr_station_id = st_arr.station_id
        WHERE st_dep.city ILIKE :dep_city AND st_arr.city ILIKE :arr_city
          {dist_filter_avion};
    """)

    result = db.execute(query, {"dep_city": dep_city, "arr_city": arr_city}).fetchall()

    if not result:
        raise HTTPException(status_code=404, detail="Aucun trajet trouvé pour ces villes.")

    response_data = []
    for row in result:
        mapping = row._mapping
        response_data.append(CompareResponse(
            mode=mapping["mode"],
            dep_city=mapping["dep_city"],
            arr_city=mapping["arr_city"],
            distance_km=float(mapping["distance_km"]) if mapping["distance_km"] is not None else 0.0,
            co2_per_km=float(mapping["co2_per_km"]) if mapping["co2_per_km"] is not None else 0.0,
            co2_kg_passenger=float(mapping["co2_kg_passenger"]) if mapping["co2_kg_passenger"] is not None else 0.0
        ))
    return response_data


@router.get("/locations/common-cities", summary="Obtenir les villes connectées par l'air et le rail")
def get_common_cities(db: Session = Depends(get_db)):
    """
    Retourne la liste des villes qui possèdent à la fois une gare et un aéroport.
    Utile pour l'ETL ou le dashboard.
    """
    query = text("""
        SELECT DISTINCT city
        FROM mart.dim_station
        WHERE city IS NOT NULL AND city != ''
        GROUP BY city
        HAVING COUNT(DISTINCT is_airport) = 2
        ORDER BY city
    """)

    result = db.execute(query).fetchall()
    return {"common_cities": [row._mapping["city"] for row in result]}


# ============================================================
# NEW ENDPOINTS
# ============================================================

@router.get("/compare/winner", response_model=WinnerResponse, summary="Mode le plus écolo pour un trajet donné")
def compare_winner(
    dep_city: str = Query(..., description="Ville de départ (ex: Paris)"),
    arr_city: str = Query(..., description="Ville d'arrivée (ex: Lyon)"),
    db: Session = Depends(get_db)
):
    """
    Retourne le mode de transport le plus écologique pour un trajet, avec les économies de CO2
    en kg et en pourcentage.
    - greener_mode: 'train' ou 'avion'
    - savings_kg: CO2 économisé en kg par passager
    - savings_percent: % de CO2 économisé par rapport au mode le plus polluant
    """
    query = text("""
        SELECT
            AVG(CASE WHEN fe.transport_mode = 'train' THEN fe.co2_kg_passenger END) AS train_co2,
            AVG(CASE WHEN fe.transport_mode = 'avion' THEN fe.co2_kg_passenger END) AS plane_co2,
            MAX(CASE WHEN fe.transport_mode = 'train' THEN st_dep.city END) AS dep_city,
            MAX(CASE WHEN fe.transport_mode = 'train' THEN st_arr.city END) AS arr_city
        FROM mart.fact_emission fe
        LEFT JOIN mart.dim_route_train rt ON fe.route_train_id = rt.route_train_id
        LEFT JOIN mart.dim_route_avion ra ON fe.route_avion_id = ra.route_avion_id
        LEFT JOIN mart.dim_station st_dep ON (
            COALESCE(rt.dep_station_id, ra.dep_station_id) = st_dep.station_id
        )
        LEFT JOIN mart.dim_station st_arr ON (
            COALESCE(rt.arr_station_id, ra.arr_station_id) = st_arr.station_id
        )
        WHERE st_dep.city ILIKE :dep_city AND st_arr.city ILIKE :arr_city
    """)

    row = db.execute(query, {"dep_city": dep_city, "arr_city": arr_city}).fetchone()

    if not row or (row._mapping["train_co2"] is None and row._mapping["plane_co2"] is None):
        raise HTTPException(status_code=404, detail="Aucun trajet comparable trouvé pour ces villes.")

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
        dep_city=m["dep_city"] or dep_city,
        arr_city=m["arr_city"] or arr_city,
        train_co2_kg=train_co2,
        plane_co2_kg=plane_co2,
        savings_kg=savings_kg,
        savings_percent=savings_pct,
    )


@router.get("/compare/passengers", response_model=List[PassengersResponse], summary="CO2 total pour N passagers avec équivalences concrètes")
def compare_passengers(
    dep_city: str = Query(..., description="Ville de départ (ex: Paris)"),
    arr_city: str = Query(..., description="Ville d'arrivée (ex: Lyon)"),
    passengers: int = Query(..., ge=1, le=500, description="Nombre de passagers (ex: 2)"),
    db: Session = Depends(get_db)
):
    """
    Calcule le CO2 total pour un groupe de N passagers sur un trajet train vs avion.

    Le champ co2_kg_passenger en base représente déjà le CO2 pour 1 passager.
    Le nombre de passagers est fourni par l'appelant — il n'est pas stocké en base.

    Équivalences fournies :
    - equivalent_trees : arbres nécessaires pour absorber ce CO2 sur 1 an (1 arbre ~ 22 kg CO2/an)
    - equivalent_car_km : kilomètres en voiture équivalents (voiture moyenne ~ 0.21 kg CO2/km)
    """
    query = text("""
        SELECT
            fe.transport_mode as mode,
            st_dep.city as dep_city,
            st_arr.city as arr_city,
            AVG(fe.co2_kg_passenger) as co2_per_passenger
        FROM mart.fact_emission fe
        LEFT JOIN mart.dim_route_train rt ON fe.route_train_id = rt.route_train_id
        LEFT JOIN mart.dim_route_avion ra ON fe.route_avion_id = ra.route_avion_id
        LEFT JOIN mart.dim_station st_dep ON (
            COALESCE(rt.dep_station_id, ra.dep_station_id) = st_dep.station_id
        )
        LEFT JOIN mart.dim_station st_arr ON (
            COALESCE(rt.arr_station_id, ra.arr_station_id) = st_arr.station_id
        )
        WHERE st_dep.city ILIKE :dep_city AND st_arr.city ILIKE :arr_city
        GROUP BY fe.transport_mode, st_dep.city, st_arr.city
        ORDER BY fe.transport_mode
    """)

    result = db.execute(query, {"dep_city": dep_city, "arr_city": arr_city}).fetchall()

    if not result:
        raise HTTPException(status_code=404, detail="Aucun trajet trouvé pour ces villes.")

    CO2_PER_TREE_KG_YEAR = 22.0   # kg CO2 absorbé par un arbre par an
    CO2_PER_CAR_KM = 0.21         # kg CO2 par km en voiture moyenne

    response_data = []
    for row in result:
        m = row._mapping
        co2_per_pax = float(m["co2_per_passenger"]) if m["co2_per_passenger"] is not None else 0.0
        total_co2 = round(co2_per_pax * passengers, 3)
        response_data.append(PassengersResponse(
            mode=m["mode"],
            dep_city=m["dep_city"],
            arr_city=m["arr_city"],
            passengers=passengers,
            total_co2_kg=total_co2,
            co2_per_passenger_kg=round(co2_per_pax, 3),
            equivalent_trees=round(total_co2 / CO2_PER_TREE_KG_YEAR, 1),
            equivalent_car_km=round(total_co2 / CO2_PER_CAR_KM, 1),
        ))
    return response_data


@router.get("/ranking/greener-routes", response_model=List[RouteRankingResponse], summary="Top N routes où le train économise le plus de CO2 vs avion")
def ranking_greener_routes(
    limit: int = Query(10, ge=1, le=100, description="Nombre de routes à retourner"),
    db: Session = Depends(get_db)
):
    """
    Retourne les routes où le train est significativement plus écologique que l'avion,
    classées par économies de CO2 décroissantes (kg par passager).
    Seules les routes avec des données pour les deux modes sont incluses.
    """
    query = text("""
        WITH train_data AS (
            SELECT
                st_dep.city AS dep_city,
                st_arr.city AS arr_city,
                AVG(fe.co2_kg_passenger) AS train_co2
            FROM mart.fact_emission fe
            JOIN mart.dim_route_train rt ON fe.route_train_id = rt.route_train_id
            JOIN mart.dim_station st_dep ON rt.dep_station_id = st_dep.station_id
            JOIN mart.dim_station st_arr ON rt.arr_station_id = st_arr.station_id
            WHERE fe.transport_mode = 'train'
            GROUP BY st_dep.city, st_arr.city
        ),
        plane_data AS (
            SELECT
                st_dep.city AS dep_city,
                st_arr.city AS arr_city,
                AVG(fe.co2_kg_passenger) AS plane_co2
            FROM mart.fact_emission fe
            JOIN mart.dim_route_avion ra ON fe.route_avion_id = ra.route_avion_id
            JOIN mart.dim_station st_dep ON ra.dep_station_id = st_dep.station_id
            JOIN mart.dim_station st_arr ON ra.arr_station_id = st_arr.station_id
            WHERE fe.transport_mode = 'avion'
            GROUP BY st_dep.city, st_arr.city
        )
        SELECT
            t.dep_city,
            t.arr_city,
            ROUND(t.train_co2::numeric, 3)  AS train_co2_kg,
            ROUND(p.plane_co2::numeric, 3)  AS plane_co2_kg,
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
        raise HTTPException(status_code=404, detail="Aucune route comparable trouvée.")

    return [
        RouteRankingResponse(
            dep_city=row._mapping["dep_city"],
            arr_city=row._mapping["arr_city"],
            train_co2_kg=float(row._mapping["train_co2_kg"]),
            plane_co2_kg=float(row._mapping["plane_co2_kg"]),
            savings_kg=float(row._mapping["savings_kg"]),
            savings_percent=float(row._mapping["savings_percent"]),
        )
        for row in result
    ]


@router.get("/search/departures", response_model=List[DepartureResponse], summary="Toutes les destinations depuis une ville avec CO2 train vs avion")
def search_departures(
    dep_city: str = Query(..., description="Ville de départ (ex: Paris)"),
    train_type: Optional[str] = Query(None, description="Type de train : 'jour', 'nuit' ou None pour tous"),
    min_distance_km: Optional[float] = Query(None, ge=0, description="Distance minimale en km (recommandé : 100)"),
    db: Session = Depends(get_db)
):
    """
    Retourne toutes les destinations disponibles depuis une ville donnée,
    avec le CO2 par passager pour chaque mode (train / avion) disponible.
    Indique également le mode le plus écologique pour chaque destination.

    - **train_type** : 'jour', 'nuit' ou None
    - **min_distance_km** : filtre les trajets trop courts
    """
    night_filter = ""
    if train_type == "nuit":
        night_filter = "AND rt.is_night_train = TRUE"
    elif train_type == "jour":
        night_filter = "AND rt.is_night_train = FALSE"

    dist_filter = f"AND rt.distance_km >= {min_distance_km}" if min_distance_km else ""
    dist_filter_avion = f"AND ra.distance_km >= {min_distance_km}" if min_distance_km else ""

    query = text(f"""
        WITH train_routes AS (
            SELECT
                st_arr.city AS arr_city,
                rt.distance_km,
                AVG(fe.co2_kg_passenger) AS train_co2
            FROM mart.fact_emission fe
            JOIN mart.dim_route_train rt ON fe.route_train_id = rt.route_train_id
            JOIN mart.dim_station st_dep ON rt.dep_station_id = st_dep.station_id
            JOIN mart.dim_station st_arr ON rt.arr_station_id = st_arr.station_id
            WHERE fe.transport_mode = 'train' AND st_dep.city ILIKE :dep_city
              {night_filter} {dist_filter}
            GROUP BY st_arr.city, rt.distance_km
        ),
        plane_routes AS (
            SELECT
                st_arr.city AS arr_city,
                ra.distance_km,
                AVG(fe.co2_kg_passenger) AS plane_co2
            FROM mart.fact_emission fe
            JOIN mart.dim_route_avion ra ON fe.route_avion_id = ra.route_avion_id
            JOIN mart.dim_station st_dep ON ra.dep_station_id = st_dep.station_id
            JOIN mart.dim_station st_arr ON ra.arr_station_id = st_arr.station_id
            WHERE fe.transport_mode = 'avion' AND st_dep.city ILIKE :dep_city
              {dist_filter_avion}
            GROUP BY st_arr.city, ra.distance_km
        )
        SELECT
            COALESCE(t.arr_city, p.arr_city) AS arr_city,
            COALESCE(t.distance_km, p.distance_km) AS distance_km,
            ROUND(t.train_co2::numeric, 3) AS train_co2_kg,
            ROUND(p.plane_co2::numeric, 3) AS plane_co2_kg,
            CASE
                WHEN t.train_co2 IS NOT NULL AND p.plane_co2 IS NOT NULL THEN
                    CASE WHEN t.train_co2 <= p.plane_co2 THEN 'train' ELSE 'avion' END
                WHEN t.train_co2 IS NOT NULL THEN 'train'
                WHEN p.plane_co2 IS NOT NULL THEN 'avion'
                ELSE NULL
            END AS greener_mode
        FROM train_routes t
        FULL OUTER JOIN plane_routes p ON t.arr_city = p.arr_city
        ORDER BY arr_city
    """)

    result = db.execute(query, {"dep_city": dep_city}).fetchall()

    if not result:
        raise HTTPException(status_code=404, detail=f"Aucune destination trouvée depuis {dep_city}.")

    return [
        DepartureResponse(
            arr_city=row._mapping["arr_city"],
            distance_km=float(row._mapping["distance_km"]) if row._mapping["distance_km"] is not None else None,
            train_co2_kg=float(row._mapping["train_co2_kg"]) if row._mapping["train_co2_kg"] is not None else None,
            plane_co2_kg=float(row._mapping["plane_co2_kg"]) if row._mapping["plane_co2_kg"] is not None else None,
            greener_mode=row._mapping["greener_mode"],
        )
        for row in result
    ]


class CoordCompareResponse(BaseModel):
    dep_lat: float
    dep_lon: float
    arr_lat: float
    arr_lon: float
    # Stations trouvées
    nearest_train_dep: Optional[str]
    nearest_train_dep_dist_km: Optional[float]
    nearest_train_arr: Optional[str]
    nearest_train_arr_dist_km: Optional[float]
    nearest_airport_dep: Optional[str]
    nearest_airport_dep_dist_km: Optional[float]
    nearest_airport_arr: Optional[str]
    nearest_airport_arr_dist_km: Optional[float]
    # CO2 par passager
    train_co2_kg: Optional[float]
    plane_co2_kg: Optional[float]
    greener_mode: Optional[str]
    savings_kg: Optional[float]
    savings_percent: Optional[float]


@router.get("/compare/coordinates", response_model=CoordCompareResponse, summary="Compare train vs avion depuis des coordonnées GPS")
def compare_coordinates(
    dep_lat: float = Query(..., ge=-90,  le=90,  description="Latitude du point de départ"),
    dep_lon: float = Query(..., ge=-180, le=180, description="Longitude du point de départ"),
    arr_lat: float = Query(..., ge=-90,  le=90,  description="Latitude du point d'arrivée"),
    arr_lon: float = Query(..., ge=-180, le=180, description="Longitude du point d'arrivée"),
    radius_km: float = Query(100.0, ge=1, le=500, description="Rayon de recherche max en km (défaut 100 km)"),
    db: Session = Depends(get_db)
):
    """
    À partir de deux points GPS (départ et arrivée), trouve :
    - La gare la plus proche de chaque point → route train
    - L'aéroport le plus proche de chaque point → route avion

    Utilise la formule Haversine pour calculer les distances.
    Compare ensuite les émissions CO2 par passager pour les deux modes.
    Retourne également les distances entre le point fourni et la station/aéroport trouvé.
    """

    # Sous-requête Haversine réutilisable : station la plus proche d'un point GPS
    # filtrée par is_airport et par un rayon maximum
    nearest_station_sql = """
        SELECT
            station_id,
            station_name,
            city,
            (2 * 6371 * asin(sqrt(
                pow(sin(radians((:lat - latitude) / 2)), 2) +
                cos(radians(:lat)) * cos(radians(latitude)) *
                pow(sin(radians((:lon - longitude) / 2)), 2)
            ))) AS dist_km
        FROM mart.dim_station
        WHERE is_airport = :is_airport
          AND latitude IS NOT NULL
          AND longitude IS NOT NULL
        ORDER BY dist_km ASC
        LIMIT 1
    """

    def find_nearest(lat, lon, is_airport):
        row = db.execute(
            text(nearest_station_sql),
            {"lat": lat, "lon": lon, "is_airport": is_airport}
        ).fetchone()
        if row is None:
            return None, None, None
        m = row._mapping
        dist = float(m["dist_km"])
        if dist > radius_km:
            return None, None, None
        return m["station_id"], m["station_name"], round(dist, 2)

    dep_train_id, dep_train_name, dep_train_dist = find_nearest(dep_lat, dep_lon, False)
    arr_train_id, arr_train_name, arr_train_dist = find_nearest(arr_lat, arr_lon, False)
    dep_airport_id, dep_airport_name, dep_airport_dist = find_nearest(dep_lat, dep_lon, True)
    arr_airport_id, arr_airport_name, arr_airport_dist = find_nearest(arr_lat, arr_lon, True)

    train_co2 = None
    plane_co2 = None

    if dep_train_id and arr_train_id:
        row = db.execute(text("""
            SELECT AVG(fe.co2_kg_passenger) AS co2
            FROM mart.fact_emission fe
            JOIN mart.dim_route_train rt ON fe.route_train_id = rt.route_train_id
            WHERE rt.dep_station_id = :dep AND rt.arr_station_id = :arr
              AND fe.transport_mode = 'train'
        """), {"dep": dep_train_id, "arr": arr_train_id}).fetchone()
        if row and row._mapping["co2"] is not None:
            train_co2 = round(float(row._mapping["co2"]), 3)

    if dep_airport_id and arr_airport_id:
        row = db.execute(text("""
            SELECT AVG(fe.co2_kg_passenger) AS co2
            FROM mart.fact_emission fe
            JOIN mart.dim_route_avion ra ON fe.route_avion_id = ra.route_avion_id
            WHERE ra.dep_station_id = :dep AND ra.arr_station_id = :arr
              AND fe.transport_mode = 'avion'
        """), {"dep": dep_airport_id, "arr": arr_airport_id}).fetchone()
        if row and row._mapping["co2"] is not None:
            plane_co2 = round(float(row._mapping["co2"]), 3)

    greener = savings_kg = savings_pct = None
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
        greener = "train"
    elif plane_co2 is not None:
        greener = "avion"

    if train_co2 is None and plane_co2 is None:
        raise HTTPException(
            status_code=404,
            detail="Aucune route trouvée entre les stations/aéroports les plus proches des coordonnées fournies."
        )

    return CoordCompareResponse(
        dep_lat=dep_lat, dep_lon=dep_lon,
        arr_lat=arr_lat, arr_lon=arr_lon,
        nearest_train_dep=dep_train_name,
        nearest_train_dep_dist_km=dep_train_dist,
        nearest_train_arr=arr_train_name,
        nearest_train_arr_dist_km=arr_train_dist,
        nearest_airport_dep=dep_airport_name,
        nearest_airport_dep_dist_km=dep_airport_dist,
        nearest_airport_arr=arr_airport_name,
        nearest_airport_arr_dist_km=arr_airport_dist,
        train_co2_kg=train_co2,
        plane_co2_kg=plane_co2,
        greener_mode=greener,
        savings_kg=savings_kg,
        savings_percent=savings_pct,
    )


@router.get("/stats/network", response_model=NetworkStatsResponse, summary="Statistiques globales du réseau train vs avion")
def stats_network(db: Session = Depends(get_db)):
    """
    Retourne des statistiques globales sur le réseau :
    - Nombre total de routes par mode
    - Nombre de routes comparables (train ET avion disponibles)
    - CO2 moyen par passager pour chaque mode
    - Économies moyennes en kg et en % sur les routes comparables
    """
    query = text("""
        WITH train_agg AS (
            SELECT
                st_dep.city AS dep_city,
                st_arr.city AS arr_city,
                AVG(fe.co2_kg_passenger) AS train_co2
            FROM mart.fact_emission fe
            JOIN mart.dim_route_train rt ON fe.route_train_id = rt.route_train_id
            JOIN mart.dim_station st_dep ON rt.dep_station_id = st_dep.station_id
            JOIN mart.dim_station st_arr ON rt.arr_station_id = st_arr.station_id
            WHERE fe.transport_mode = 'train'
            GROUP BY st_dep.city, st_arr.city
        ),
        plane_agg AS (
            SELECT
                st_dep.city AS dep_city,
                st_arr.city AS arr_city,
                AVG(fe.co2_kg_passenger) AS plane_co2
            FROM mart.fact_emission fe
            JOIN mart.dim_route_avion ra ON fe.route_avion_id = ra.route_avion_id
            JOIN mart.dim_station st_dep ON ra.dep_station_id = st_dep.station_id
            JOIN mart.dim_station st_arr ON ra.arr_station_id = st_arr.station_id
            WHERE fe.transport_mode = 'avion'
            GROUP BY st_dep.city, st_arr.city
        ),
        comparable AS (
            SELECT
                t.train_co2,
                p.plane_co2,
                (p.plane_co2 - t.train_co2) AS savings
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
        raise HTTPException(status_code=500, detail="Impossible de calculer les statistiques réseau.")

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
