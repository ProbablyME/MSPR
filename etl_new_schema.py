from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, lead,
    radians, sin, cos, asin, sqrt, when, upper, concat_ws,
    collect_list, sort_array, concat, md5, coalesce, broadcast
)
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, FloatType, IntegerType
from math import radians as math_radians, sin as math_sin, cos as math_cos, asin as math_asin, sqrt as math_sqrt
import os
import sys
import gc
import re
import logging
import subprocess
import tempfile
import requests
import zipfile
from datetime import datetime
from collections import defaultdict

# ── Logging ──────────────────────────────────────────────────
LOG_FILE = f"etl_new_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# ── Config PostgreSQL ────────────────────────────────────────
DB_CONFIG = {
    "host": "localhost",
    "port": "5432",
    "database": "postgres",
    "user": "postgres",
    "password": "lpironti"
}

PSQL_PATH = "/Library/PostgreSQL/18/bin/psql"
GTFS_DIR = "downloads_gtfs"
FEEDS_CSV = "feeds_v2.csv"
BACKONTRACK_DIR = "backontrack"
SCHEMA = "mart"

# ── Pays européens (ISO 3166-1 alpha-2) ──────────────────────
# EUROPEAN_COUNTRIES = {
#     'AL', 'AD', 'AT', 'BY', 'BE', 'BA', 'BG', 'HR', 'CY', 'CZ',
#     'DK', 'EE', 'FI', 'FR', 'DE', 'GR', 'HU', 'IS', 'IE', 'IT',
#     'XK', 'LV', 'LI', 'LT', 'LU', 'MT', 'MD', 'MC', 'ME', 'NL',
#     'MK', 'NO', 'PL', 'PT', 'RO', 'RU', 'SM', 'RS', 'SK', 'SI',
#     'ES', 'SE', 'CH', 'UA', 'GB', 'VA'
# }

EUROPEAN_COUNTRIES = {
    'FR'
}

# ── Types de routes GTFS (codes étendus) ──────────────────────
# On ne garde que les services voyageurs pertinents.
# Exclus : 104 (transport voitures), 110 (substitution), 111 (spécial),
#          112 (transport camions), 113/114/115/117 (génériques/non pertinents)

# Grande vitesse → CO2 TGV
TGV_ROUTE_TYPES = {101}  # 101 = Grande vitesse (TGV, ICE, Eurostar)

# Mots-clés dans route_short_name / route_long_name pour identifier la grande vitesse
TGV_KEYWORDS = [
    "TGV", "OUIGO", "INOUI",          # France
    "ICE",                              # Allemagne
    "EUROSTAR", "THALYS",               # International
    "AVE",                              # Espagne
    "FRECCIAROSSA", "FRECCIARGENTO",    # Italie
    "ITALO",                            # Italie (NTV)
    "RAILJET",                          # Autriche (ÖBB)
    "FYRA",                             # Pays-Bas/Belgique
    "PENDOLINO",                        # Divers pays
    "KTX",                              # Corée (au cas où)
    "SAPSAN",                           # Russie
    "SJ 2000",                          # Suède
]

# Intercité / grandes lignes → CO2 Intercité
INTERCITE_ROUTE_TYPES = {
    2,    # Train générique GTFS
    100,  # Service de chemin de fer (générique étendu)
    102,  # Grandes lignes (InterCity/EuroCity)
    103,  # Transrégional (InterRegio)
    105,  # Couchettes (trains de nuit voyageurs)
    106,  # Régional (TER, Regionalzug)
}

ALL_TRAIN_ROUTE_TYPES = TGV_ROUTE_TYPES | INTERCITE_ROUTE_TYPES

# ── CO2 par 100km (en kg) ────────────────────────────────────
CO2_TGV_PER_100KM = 0.29
CO2_INTERCITE_PER_100KM = 0.90

# ── Seuil de déduplication stations ──────────────────────────
# Deux stations à moins de STATION_DEDUP_KM km l'une de l'autre
# sont considérées comme la même gare physique
STATION_DEDUP_KM = 0.15  # 150 mètres

# Distance max pour considérer que deux stations de même nom
# sont la même gare (sinon homonymes dans des villes différentes)
MAX_NAME_DEDUP_KM = 50.0

# Tables à vider (ordre FK)
TABLES_TO_TRUNCATE = [
    f"{SCHEMA}.fact_emission",
    f"{SCHEMA}.dim_route_train", f"{SCHEMA}.dim_route_avion",
    f"{SCHEMA}.dim_vehicle_train", f"{SCHEMA}.dim_vehicle_avion",
    f"{SCHEMA}.dim_station"
]

# ── Latitude / Longitude valides ─────────────────────────────
# On exclut les coordonnées nulles, à 0, ou hors limites raisonnables
MIN_LAT, MAX_LAT = -90.0, 90.0
MIN_LON, MAX_LON = -180.0, 180.0
# Seuil pour considérer une coordonnée comme "fausse" (trop proche de 0)
COORD_ZERO_THRESHOLD = 0.1


# ═════════════════════════════════════════════════════════════
#  UTILITAIRES
# ═════════════════════════════════════════════════════════════

def get_spark_session():
    return SparkSession.builder \
        .appName("ETL New Schema") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "8g") \
        .getOrCreate()


# ═════════════════════════════════════════════════════════════
#  TÉLÉCHARGEMENT GTFS
# ═════════════════════════════════════════════════════════════

def download_and_extract(url: str, country_code: str, feed_id: str) -> bool:
    """Télécharge un fichier ZIP GTFS et l'extrait dans downloads_gtfs/{country}/{feed_id}."""
    try:
        response = requests.get(url, timeout=30, stream=True)
        response.raise_for_status()

        country_dir = os.path.join(GTFS_DIR, country_code)
        os.makedirs(country_dir, exist_ok=True)

        temp_zip = os.path.join(country_dir, f"{feed_id}.zip")
        with open(temp_zip, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        extract_dir = os.path.join(country_dir, feed_id)
        os.makedirs(extract_dir, exist_ok=True)

        with zipfile.ZipFile(temp_zip, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)

        os.remove(temp_zip)
        logger.info(f"  Téléchargé: {country_code}/{feed_id}")
        return True
    except Exception as e:
        logger.error(f"  Erreur téléchargement {country_code}/{feed_id}: {e}")
        return False


def download_all_gtfs(spark):
    """Télécharge tous les feeds GTFS européens depuis feeds_v2.csv."""
    os.makedirs(GTFS_DIR, exist_ok=True)

    df = spark.read.csv(FEEDS_CSV, header=True, inferSchema=True)

    df_europe = df.filter(
        (col("`location.country_code`").isin(list(EUROPEAN_COUNTRIES))) &
        (col("`urls.latest`").isNotNull()) &
        (col("status") == "active")
    ).select(
        col("id"),
        col("`location.country_code`").alias("country_code"),
        col("`urls.latest`").alias("url"),
        col("name")
    )

    logger.info(f"Feeds européens trouvés: {df_europe.count()}")

    feeds = df_europe.collect()
    success = 0
    for feed in feeds:
        feed_dir = os.path.join(GTFS_DIR, feed['country_code'], str(feed['id']))
        if os.path.isdir(feed_dir):
            logger.info(f"  Déjà présent: {feed['country_code']}/{feed['id']}, skip")
            success += 1
            continue
        if download_and_extract(feed['url'], feed['country_code'], str(feed['id'])):
            success += 1

    logger.info(f"Téléchargement terminé: {success}/{len(feeds)}")


def run_sql(sql_command: str) -> bool:
    env = os.environ.copy()
    env["PGPASSWORD"] = DB_CONFIG["password"]
    result = subprocess.run(
        [PSQL_PATH, "-h", DB_CONFIG["host"], "-p", DB_CONFIG["port"],
         "-U", DB_CONFIG["user"], "-d", DB_CONFIG["database"],
         "-c", sql_command],
        env=env, capture_output=True, text=True
    )
    if result.returncode != 0:
        logger.error(f"Erreur psql: {result.stderr}")
        return False
    return True


def ensure_schema():
    logger.info(f"Création du schéma {SCHEMA} si nécessaire...")
    return run_sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};")


def truncate_tables():
    logger.info("Vidage des tables...")
    tables_list = ", ".join(TABLES_TO_TRUNCATE)
    return run_sql(f"TRUNCATE TABLE {tables_list} RESTART IDENTITY CASCADE;")


def list_gtfs_feeds(base_dir: str) -> list:
    feeds = []
    for country_code in os.listdir(base_dir):
        country_path = os.path.join(base_dir, country_code)
        if os.path.isdir(country_path):
            for feed_id in os.listdir(country_path):
                feed_path = os.path.join(country_path, feed_id)
                if os.path.isdir(feed_path):
                    feeds.append({
                        "country_code": country_code,
                        "feed_id": feed_id,
                        "path": feed_path
                    })
    return feeds


def read_gtfs_file(spark, feed_path: str, filename: str):
    filepath = os.path.join(feed_path, filename)
    if os.path.exists(filepath):
        return spark.read.csv(filepath, header=True, inferSchema=True)
    return None


def is_valid_coord(lat_col, lon_col):
    """Retourne une condition Spark qui filtre les coordonnées invalides."""
    return (
        lat_col.isNotNull() & lon_col.isNotNull() &
        # Pas trop proche de 0 (données bidon)
        ~((lat_col.between(-COORD_ZERO_THRESHOLD, COORD_ZERO_THRESHOLD)) &
          (lon_col.between(-COORD_ZERO_THRESHOLD, COORD_ZERO_THRESHOLD))) &
        # Dans les bornes réelles
        lat_col.between(MIN_LAT, MAX_LAT) &
        lon_col.between(MIN_LON, MAX_LON)
    )


def haversine_col(lat1, lon1, lat2, lon2):
    """Calcul de la distance haversine en km entre deux colonnes lat/lon."""
    R = 6371.0  # rayon de la Terre en km
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    return lit(2 * R) * asin(sqrt(a))


def haversine_python(lat1, lon1, lat2, lon2):
    """Calcul de la distance haversine en km (version Python pure pour le clustering)."""
    R = 6371.0
    dlat = math_radians(lat2 - lat1)
    dlon = math_radians(lon2 - lon1)
    a = math_sin(dlat / 2) ** 2 + math_cos(math_radians(lat1)) * math_cos(math_radians(lat2)) * math_sin(dlon / 2) ** 2
    return 2 * R * math_asin(math_sqrt(a))


def normalize_station_name(name: str) -> str:
    """Normalise un nom de station pour la déduplication cross-feeds.
    Supprime les codes trailing (ABC), normalise les espaces, strip."""
    if not name:
        return ""
    # Supprimer les codes entre parenthèses/crochets en fin de nom
    name = re.sub(r"\s*[\(\[][A-Z0-9]{2,5}[\)\]]\s*$", "", name)
    # Normaliser les espaces
    name = re.sub(r"\s+", " ", name).strip()
    return name


def deduplicate_stations(stations_rows):
    """
    Regroupe les stations proches géographiquement (< STATION_DEDUP_KM).
    Utilise un algorithme Union-Find pour construire des clusters.
    Retourne un dict {station_id_doublon: station_id_canonique}.
    Le station_id canonique est celui avec le nom le plus long (le plus descriptif).
    """
    stations = []
    for row in stations_rows:
        stations.append({
            "station_id": row["station_id"],
            "station_name": row["station_name"] or "",
            "latitude": float(row["latitude"]),
            "longitude": float(row["longitude"])
        })

    n = len(stations)
    if n == 0:
        return {}

    # Union-Find
    parent = list(range(n))

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(x, y):
        px, py = find(x), find(y)
        if px != py:
            parent[px] = py

    # Comparer toutes les paires et fusionner si < seuil
    # Optimisation : trier par latitude pour réduire les comparaisons
    sorted_indices = sorted(range(n), key=lambda i: stations[i]["latitude"])

    for idx_i in range(len(sorted_indices)):
        i = sorted_indices[idx_i]
        lat_i = stations[i]["latitude"]
        lon_i = stations[i]["longitude"]

        for idx_j in range(idx_i + 1, len(sorted_indices)):
            j = sorted_indices[idx_j]
            lat_j = stations[j]["latitude"]

            # Si la différence de latitude dépasse le seuil en degrés (~0.005° ≈ 0.55km),
            # toutes les stations suivantes seront encore plus loin → on arrête
            if abs(lat_j - lat_i) > 0.01:
                break

            lon_j = stations[j]["longitude"]
            dist = haversine_python(lat_i, lon_i, lat_j, lon_j)
            if dist < STATION_DEDUP_KM:
                union(i, j)

    # Construire les clusters
    clusters = {}
    for i in range(n):
        root = find(i)
        if root not in clusters:
            clusters[root] = []
        clusters[root].append(i)

    # Pour chaque cluster, choisir le station_id canonique (nom le plus long)
    station_id_mapping = {}  # {old_id: canonical_id}
    dedup_count = 0
    for members in clusters.values():
        if len(members) == 1:
            continue
        # Trier par longueur de nom décroissante, puis alphabétiquement
        members.sort(key=lambda i: (-len(stations[i]["station_name"]), stations[i]["station_id"]))
        canonical = stations[members[0]]["station_id"]
        for m in members[1:]:
            station_id_mapping[stations[m]["station_id"]] = canonical
            dedup_count += 1

    if dedup_count > 0:
        logger.info(f"  Déduplication géo: {dedup_count} doublons fusionnés en {len(clusters)} stations uniques")

    return station_id_mapping


def write_to_postgres_upsert(df, table_name: str, conflict_cols: str):
    """INSERT … ON CONFLICT (conflict_cols) DO NOTHING."""
    if df is None:
        return 0
    count = df.count()
    if count == 0:
        return 0

    logger.info(f"Écriture de {count} lignes dans {table_name}...")
    rows = df.collect()
    columns = df.columns

    try:
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            temp_file = f.name
            for row in rows:
                values = []
                for c in columns:
                    val = row[c]
                    if val is None:
                        values.append("NULL")
                    elif isinstance(val, bool):
                        values.append("TRUE" if val else "FALSE")
                    elif isinstance(val, (int, float)):
                        values.append(str(val))
                    else:
                        values.append(f"'{str(val).replace(chr(39), chr(39)*2)}'")
                cols_str = ", ".join(f'"{c}"' for c in columns)
                vals_str = ", ".join(values)
                f.write(f"INSERT INTO {table_name} ({cols_str}) VALUES ({vals_str}) ON CONFLICT ({conflict_cols}) DO NOTHING;\n")

        env = os.environ.copy()
        env["PGPASSWORD"] = DB_CONFIG["password"]
        result = subprocess.run(
            [PSQL_PATH, "-h", DB_CONFIG["host"], "-p", DB_CONFIG["port"],
             "-U", DB_CONFIG["user"], "-d", DB_CONFIG["database"],
             "-f", temp_file],
            env=env, capture_output=True, text=True
        )
        os.remove(temp_file)

        if result.returncode == 0:
            logger.info(f"  OK: {table_name} – {count} lignes")
            return count
        else:
            logger.error(f"  Erreur psql: {result.stderr[:500]}")
            return 0
    except Exception as e:
        logger.error(f"  Erreur {table_name}: {e}")
        return 0


def write_to_postgres_simple(df, table_name: str):
    """INSERT simple (pour tables avec SERIAL PK)."""
    if df is None:
        return 0
    count = df.count()
    if count == 0:
        return 0

    logger.info(f"Écriture de {count} lignes dans {table_name}...")
    rows = df.collect()
    columns = df.columns

    try:
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            temp_file = f.name
            for row in rows:
                values = []
                for c in columns:
                    val = row[c]
                    if val is None:
                        values.append("NULL")
                    elif isinstance(val, bool):
                        values.append("TRUE" if val else "FALSE")
                    elif isinstance(val, (int, float)):
                        values.append(str(val))
                    else:
                        values.append(f"'{str(val).replace(chr(39), chr(39)*2)}'")
                cols_str = ", ".join(f'"{c}"' for c in columns)
                vals_str = ", ".join(values)
                f.write(f"INSERT INTO {table_name} ({cols_str}) VALUES ({vals_str});\n")

        env = os.environ.copy()
        env["PGPASSWORD"] = DB_CONFIG["password"]
        result = subprocess.run(
            [PSQL_PATH, "-h", DB_CONFIG["host"], "-p", DB_CONFIG["port"],
             "-U", DB_CONFIG["user"], "-d", DB_CONFIG["database"],
             "-f", temp_file],
            env=env, capture_output=True, text=True
        )
        os.remove(temp_file)

        if result.returncode == 0:
            logger.info(f"  OK: {table_name} – {count} lignes")
            return count
        else:
            logger.error(f"  Erreur psql: {result.stderr[:500]}")
            return 0
    except Exception as e:
        logger.error(f"  Erreur {table_name}: {e}")
        return 0


def write_returning_ids(df, table_name: str, returning_col: str, conflict_cols: str = None):
    """INSERT et récupère les IDs générés (SERIAL). Retourne un dict mapping clé -> id."""
    if df is None:
        return {}
    count = df.count()
    if count == 0:
        return {}

    logger.info(f"Écriture de {count} lignes dans {table_name} (avec RETURNING)...")
    rows = df.collect()
    columns = df.columns

    try:
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            temp_file = f.name
            for row in rows:
                values = []
                for c in columns:
                    val = row[c]
                    if val is None:
                        values.append("NULL")
                    elif isinstance(val, bool):
                        values.append("TRUE" if val else "FALSE")
                    elif isinstance(val, (int, float)):
                        values.append(str(val))
                    else:
                        values.append(f"'{str(val).replace(chr(39), chr(39)*2)}'")
                cols_str = ", ".join(f'"{c}"' for c in columns)
                vals_str = ", ".join(values)
                if conflict_cols:
                    f.write(f"INSERT INTO {table_name} ({cols_str}) VALUES ({vals_str}) ON CONFLICT ({conflict_cols}) DO NOTHING;\n")
                else:
                    f.write(f"INSERT INTO {table_name} ({cols_str}) VALUES ({vals_str});\n")

        env = os.environ.copy()
        env["PGPASSWORD"] = DB_CONFIG["password"]
        result = subprocess.run(
            [PSQL_PATH, "-h", DB_CONFIG["host"], "-p", DB_CONFIG["port"],
             "-U", DB_CONFIG["user"], "-d", DB_CONFIG["database"],
             "-f", temp_file],
            env=env, capture_output=True, text=True
        )
        os.remove(temp_file)

        if result.returncode == 0:
            logger.info(f"  OK: {table_name} – {count} lignes")
        else:
            logger.error(f"  Erreur psql: {result.stderr[:500]}")

    except Exception as e:
        logger.error(f"  Erreur {table_name}: {e}")


def query_sql(sql_command: str) -> str:
    """Exécute une requête SELECT et retourne le résultat brut."""
    env = os.environ.copy()
    env["PGPASSWORD"] = DB_CONFIG["password"]
    result = subprocess.run(
        [PSQL_PATH, "-h", DB_CONFIG["host"], "-p", DB_CONFIG["port"],
         "-U", DB_CONFIG["user"], "-d", DB_CONFIG["database"],
         "-t", "-A", "-c", sql_command],
        env=env, capture_output=True, text=True
    )
    if result.returncode != 0:
        logger.error(f"Erreur query: {result.stderr}")
        return ""
    return result.stdout.strip()


# ═════════════════════════════════════════════════════════════
#  PHASE 1 : COLLECTE DES STATIONS
# ═════════════════════════════════════════════════════════════

def get_train_stop_ids(spark, feed_path: str):
    """Retourne l'ensemble des stop_id utilisés par des trips ferroviaires dans ce feed."""
    routes_df = read_gtfs_file(spark, feed_path, "routes.txt")
    trips_df = read_gtfs_file(spark, feed_path, "trips.txt")
    stop_times_df = read_gtfs_file(spark, feed_path, "stop_times.txt")

    if routes_df is None or trips_df is None or stop_times_df is None:
        return set()

    # Filtrer les routes ferroviaires
    train_routes = routes_df.filter(
        col("route_type").cast(IntegerType()).isin(list(ALL_TRAIN_ROUTE_TYPES))
    ).select("route_id")

    if train_routes.count() == 0:
        return set()

    # Trips ferroviaires
    train_trips = trips_df.join(train_routes, "route_id", "semi").select("trip_id")

    # Stop_ids utilisés par ces trips
    train_stop_ids = stop_times_df.join(train_trips, "trip_id", "semi") \
        .select(col("stop_id").cast(StringType())).distinct()

    return set(r.stop_id for r in train_stop_ids.collect())


def collect_stations_from_feed(spark, feed_path: str, country_code: str, train_stop_ids: set) -> list:
    """
    Extrait les métadonnées de stations d'un feed.
    Résout parent_station : si un arrêt a un parent, on utilise le parent.
    Retourne une liste légère de dicts Python (pas de Spark DataFrame).
    """
    df = read_gtfs_file(spark, feed_path, "stops.txt")
    if df is None:
        return []

    # Garder location_type 0, 1 ou null
    if "location_type" in df.columns:
        df = df.filter((col("location_type").isNull()) | (col("location_type").isin([0, 1])))

    # Caster coordonnées
    df = df.withColumn("lat", col("stop_lat").cast(FloatType())) \
           .withColumn("lon", col("stop_lon").cast(FloatType()))
    df = df.filter(is_valid_coord(col("lat"), col("lon")))

    has_parent = "parent_station" in df.columns

    select_cols = [
        col("stop_id").cast(StringType()).alias("stop_id"),
        col("stop_name").cast(StringType()).alias("station_name"),
        col("lat").alias("latitude"),
        col("lon").alias("longitude"),
    ]
    if has_parent:
        select_cols.append(col("parent_station").cast(StringType()).alias("parent_station"))

    rows = df.select(*select_cols).collect()

    # Lookup stop_id -> row
    stop_lookup = {}
    for row in rows:
        stop_lookup[row["stop_id"]] = row

    # Étendre les train_stop_ids pour inclure les parents
    expanded_ids = set(train_stop_ids)
    if has_parent:
        for sid in train_stop_ids:
            if sid in stop_lookup:
                parent = stop_lookup[sid]["parent_station"]
                if parent and parent in stop_lookup:
                    expanded_ids.add(parent)

    # Construire le résultat : résoudre chaque stop vers son parent
    result = []
    seen_resolved = set()

    for sid in expanded_ids:
        if sid not in stop_lookup:
            continue
        row = stop_lookup[sid]

        # Résoudre vers le parent si c'est un quai (a un parent_station)
        resolved_id = sid
        resolved_row = row
        if has_parent and row["parent_station"]:
            parent_id = row["parent_station"]
            if parent_id in stop_lookup:
                resolved_id = parent_id
                resolved_row = stop_lookup[parent_id]

        if resolved_id in seen_resolved:
            continue
        seen_resolved.add(resolved_id)

        name = resolved_row["station_name"] or ""
        result.append({
            "resolved_stop_id": resolved_id,
            "station_name": name,
            "normalized_name": normalize_station_name(name),
            "latitude": float(resolved_row["latitude"]),
            "longitude": float(resolved_row["longitude"]),
            "country_code": country_code,
        })

    return result


def collect_stations_from_backontrack(spark) -> list:
    """
    Collecte les stations depuis les CSV Back on Track.
    Ne garde que les stations utilisées dans les trip_stops.
    Retourne une liste de dicts au même format que collect_stations_from_feed().
    """
    stops_path = os.path.join(BACKONTRACK_DIR, "back_on_track_stops.csv")
    trip_stop_path = os.path.join(BACKONTRACK_DIR, "back_on_track_trip_stop.csv")

    if not os.path.exists(stops_path) or not os.path.exists(trip_stop_path):
        logger.warning("Back on Track: fichiers manquants, ignoré")
        return []

    # Lire les trip_stops pour savoir quelles stations sont utilisées
    trip_stop_df = spark.read.csv(trip_stop_path, header=True, inferSchema=True)
    used_stop_ids = set(
        r.stop_id for r in trip_stop_df.select(
            col("stop_id").cast(StringType())
        ).distinct().collect()
    )

    # Lire les stops
    stops_df = spark.read.csv(stops_path, header=True, inferSchema=True)
    stops_df = stops_df.withColumn("lat", col("station_lat").cast(FloatType())) \
                       .withColumn("lon", col("station_long").cast(FloatType()))
    stops_df = stops_df.filter(is_valid_coord(col("lat"), col("lon")))

    rows = stops_df.select(
        col("stop_id").cast(StringType()).alias("stop_id"),
        col("station_name").cast(StringType()).alias("station_name"),
        col("lat").alias("latitude"),
        col("lon").alias("longitude"),
        col("stop_country").cast(StringType()).alias("country_code"),
    ).collect()

    result = []
    for row in rows:
        sid = row["stop_id"]
        if sid not in used_stop_ids:
            continue
        name = row["station_name"] or sid
        cc = row["country_code"] or ""
        result.append({
            "resolved_stop_id": sid,
            "station_name": name,
            "normalized_name": normalize_station_name(name),
            "latitude": float(row["latitude"]),
            "longitude": float(row["longitude"]),
            "country_code": cc,
        })

    return result


# ═════════════════════════════════════════════════════════════
#  PHASE 2 : DÉDUPLICATION GLOBALE
# ═════════════════════════════════════════════════════════════

def build_global_station_registry(all_stations: list):
    """
    Déduplication globale des stations en 2 couches :
    1. Nom normalisé : stations avec le même nom normalisé (et < MAX_NAME_DEDUP_KM) sont fusionnées
    2. Proximité géographique : Union-Find < STATION_DEDUP_KM (150m)

    (La couche parent_station a déjà été appliquée dans collect_stations_from_feed)

    Retourne (canonical_stations, global_mapping)
    - canonical_stations: liste de dicts pour insertion dans dim_station
    - global_mapping: dict {resolved_stop_id: canonical_station_id}
    """
    # --- Couche 1 : Déduplication par nom normalisé ---
    name_groups = defaultdict(list)
    for station in all_stations:
        key = station["normalized_name"].lower()
        if key:
            name_groups[key].append(station)
        else:
            # Noms vides : pas de regroupement par nom
            name_groups[f"__empty_{station['resolved_stop_id']}"].append(station)

    merged_stations = []
    name_dedup_count = 0

    for name_key, group in name_groups.items():
        if len(group) == 1:
            merged_stations.append(group[0])
            continue

        # Vérifier que les stations du groupe sont proches géographiquement
        # Sinon c'est un homonyme (ex: "Gare" dans deux villes différentes)
        # On sous-groupe par proximité
        sub_groups = []
        used = [False] * len(group)

        for i in range(len(group)):
            if used[i]:
                continue
            cluster = [i]
            used[i] = True
            for j in range(i + 1, len(group)):
                if used[j]:
                    continue
                dist = haversine_python(
                    group[i]["latitude"], group[i]["longitude"],
                    group[j]["latitude"], group[j]["longitude"]
                )
                if dist < MAX_NAME_DEDUP_KM:
                    cluster.append(j)
                    used[j] = True
            sub_groups.append(cluster)

        for cluster_indices in sub_groups:
            cluster = [group[i] for i in cluster_indices]
            # Choisir la station canonique : nom le plus long
            cluster.sort(key=lambda s: (-len(s["station_name"]), s["resolved_stop_id"]))
            canonical = cluster[0].copy()

            # Collecter tous les resolved_stop_id du cluster
            canonical["merged_ids"] = set(s["resolved_stop_id"] for s in cluster)
            merged_stations.append(canonical)

            if len(cluster) > 1:
                name_dedup_count += len(cluster) - 1

    if name_dedup_count > 0:
        logger.info(f"  Déduplication par nom: {name_dedup_count} doublons fusionnés")

    # --- Couche 2 : Déduplication géographique (Union-Find < 150m) ---
    stations_for_geo = []
    for s in merged_stations:
        stations_for_geo.append({
            "station_id": s["resolved_stop_id"],
            "station_name": s["station_name"],
            "latitude": s["latitude"],
            "longitude": s["longitude"],
        })

    geo_mapping = deduplicate_stations(stations_for_geo)

    # --- Construire le registre final ---
    # Appliquer le geo_mapping pour résoudre les derniers doublons
    canonical_by_id = {}
    for s in merged_stations:
        rid = s["resolved_stop_id"]
        final_id = geo_mapping.get(rid, rid)
        if final_id not in canonical_by_id:
            canonical_by_id[final_id] = s.copy()
            canonical_by_id[final_id]["resolved_stop_id"] = final_id

    # Construire le mapping global : tout resolved_stop_id -> canonical_station_id
    global_mapping = {}
    for s in merged_stations:
        rid = s["resolved_stop_id"]
        final_id = geo_mapping.get(rid, rid)

        # Mapper le resolved_stop_id lui-même
        if rid != final_id:
            global_mapping[rid] = final_id

        # Mapper tous les IDs fusionnés par nom
        for merged_id in s.get("merged_ids", set()):
            if merged_id != final_id:
                global_mapping[merged_id] = final_id

    # Construire la liste des stations canoniques pour dim_station
    canonical_stations = []
    for cid, s in canonical_by_id.items():
        canonical_stations.append({
            "station_id": cid,
            "station_name": s["station_name"],
            "city": s["station_name"],
            "country_code": s["country_code"],
            "latitude": s["latitude"],
            "longitude": s["longitude"],
            "is_airport": False,
        })

    logger.info(f"  Registre global: {len(canonical_stations)} stations canoniques, "
                f"{len(global_mapping)} remappings")

    return canonical_stations, global_mapping


def build_feed_stop_id_mapping(spark, feed_path: str, global_mapping: dict) -> dict:
    """
    Construit le mapping complet stop_id -> canonical_station_id pour un feed.
    Résout parent_station dans le feed, puis applique le mapping global.
    """
    df = read_gtfs_file(spark, feed_path, "stops.txt")
    if df is None:
        return {}

    has_parent = "parent_station" in df.columns

    select_cols = [col("stop_id").cast(StringType()).alias("stop_id")]
    if has_parent:
        select_cols.append(col("parent_station").cast(StringType()).alias("parent_station"))

    rows = df.select(*select_cols).collect()

    mapping = {}
    for row in rows:
        sid = row["stop_id"]

        # Étape 1 : résoudre vers le parent dans ce feed
        resolved = sid
        if has_parent and row["parent_station"]:
            resolved = row["parent_station"]

        # Étape 2 : appliquer le mapping global
        final = global_mapping.get(resolved, resolved)

        # Vérifier aussi si le stop_id original est directement dans le mapping global
        if sid in global_mapping:
            final = global_mapping[sid]

        if sid != final:
            mapping[sid] = final

    return mapping


# ═════════════════════════════════════════════════════════════
#  PHASE 3 : TRAITEMENT DES ROUTES ET FAITS
# ═════════════════════════════════════════════════════════════

def process_train_routes_and_facts(spark, feed_path: str, feed_id: str, country_code: str, station_id_mapping: dict = None):
    """
    Extrait pour chaque trip train les tronçons consécutifs (arrêt N → arrêt N+1),
    calcule la distance haversine, et produit les données pour dim_route_train + fact_emission.

    station_id_mapping: dict {old_id: canonical_id} pour remplacer les stations doublons.
    Déduplique les trajets par signature (séquence complète des arrêts canoniques).

    Retourne (routes_df, facts_df) ou (None, None).
    """
    if station_id_mapping is None:
        station_id_mapping = {}
    # 1) Lire routes.txt et filtrer les trains
    routes_df = read_gtfs_file(spark, feed_path, "routes.txt")
    if routes_df is None:
        return None, None

    routes_df = routes_df.filter(
        col("route_type").cast(IntegerType()).isin(list(ALL_TRAIN_ROUTE_TYPES))
    )
    if routes_df.count() == 0:
        return None, None

    # Garder le mapping route_id -> route_type + noms pour détection TGV
    route_name_cols = ["route_id", "route_type"]
    if "route_short_name" in routes_df.columns:
        route_name_cols.append("route_short_name")
    if "route_long_name" in routes_df.columns:
        route_name_cols.append("route_long_name")

    route_type_df = routes_df.select(
        *[col(c).cast(StringType()) if c != "route_type" else col(c).cast(IntegerType()) for c in route_name_cols]
    ).dropDuplicates(["route_id"])

    train_route_ids = [r.route_id for r in route_type_df.collect()]

    # 2) Lire trips, stop_times, stops
    trips_df = read_gtfs_file(spark, feed_path, "trips.txt")
    stop_times_df = read_gtfs_file(spark, feed_path, "stop_times.txt")
    stops_df = read_gtfs_file(spark, feed_path, "stops.txt")

    if trips_df is None or stop_times_df is None or stops_df is None:
        return None, None

    # Filtrer trips ferroviaires
    trips_df = trips_df.filter(col("route_id").cast(StringType()).isin(train_route_ids))
    if trips_df.count() == 0:
        return None, None

    # 2b) Appliquer le mapping de stations dédupliquées sur les stop_id
    # On utilise un broadcast join (scalable) au lieu de create_map (OOM sur gros feeds)
    if station_id_mapping:
        mapping_data = [(old_id, new_id) for old_id, new_id in station_id_mapping.items()]
        mapping_df = spark.createDataFrame(mapping_data, ["old_stop_id", "canonical_stop_id"])

        # Remapper stop_times
        stop_times_df = stop_times_df.join(
            broadcast(mapping_df),
            stop_times_df["stop_id"].cast(StringType()) == mapping_df["old_stop_id"],
            "left"
        ).withColumn(
            "stop_id",
            coalesce(col("canonical_stop_id"), col("stop_id"))
        ).drop("old_stop_id", "canonical_stop_id")

        # Remapper stops
        stops_df = stops_df.join(
            broadcast(mapping_df),
            stops_df["stop_id"].cast(StringType()) == mapping_df["old_stop_id"],
            "left"
        ).withColumn(
            "stop_id",
            coalesce(col("canonical_stop_id"), col("stop_id"))
        ).drop("old_stop_id", "canonical_stop_id")

        # Dédupliquer stops après remapping (garder le premier)
        stops_df = stops_df.dropDuplicates(["stop_id"])

    # 3) Créer une signature de trajet basée sur la séquence complète des arrêts
    # Deux trips avec la même séquence d'arrêts (même stations dans le même ordre) = même trajet
    stop_times_train = stop_times_df.join(
        trips_df.select(col("trip_id"), col("route_id")),
        "trip_id", "inner"
    )

    # Signature = hash de la concaténation ordonnée de tous les stop_id du trip
    trip_signatures = stop_times_train.groupBy("trip_id", "route_id").agg(
        sort_array(collect_list(
            concat(col("stop_sequence").cast(StringType()), lit(":"), col("stop_id").cast(StringType()))
        )).alias("stops_ordered")
    ).withColumn(
        "trip_signature", md5(concat_ws("|", col("stops_ordered")))
    ).drop("stops_ordered")

    # Dédupliquer les trips par signature : on ne garde qu'un trip par signature unique
    trip_signatures = trip_signatures.dropDuplicates(["trip_signature"])
    dedup_count_before = trips_df.count()
    dedup_count_after = trip_signatures.count()
    if dedup_count_before != dedup_count_after:
        logger.info(f"  Déduplication trajets: {dedup_count_before} → {dedup_count_after} trips uniques (par séquence d'arrêts)")

    # 4) Pour chaque trip unique, créer les tronçons consécutifs (arrêt N → arrêt N+1)
    # Au lieu de prendre seulement premier→dernier arrêt, on garde chaque paire consécutive
    unique_trip_ids = trip_signatures.select("trip_id", "route_id")
    stop_times_unique = stop_times_train.join(
        unique_trip_ids.select("trip_id"), "trip_id", "semi"
    )

    # Self-join : chaque arrêt avec l'arrêt suivant dans le même trip
    # On utilise lead via Window pour trouver le stop suivant
    w_trip = Window.partitionBy("trip_id").orderBy("stop_sequence")

    stop_pairs = stop_times_unique.withColumn(
        "next_stop_id", lead("stop_id").over(w_trip)
    ).filter(
        col("next_stop_id").isNotNull()  # Exclure le dernier arrêt (pas de suivant)
    ).select(
        col("trip_id"),
        col("stop_id").cast(StringType()).alias("dep_stop_id"),
        col("next_stop_id").cast(StringType()).alias("arr_stop_id")
    )

    # Exclure les tronçons où départ == arrivée (même station après dédup)
    stop_pairs = stop_pairs.filter(col("dep_stop_id") != col("arr_stop_id"))

    # Joindre avec route_id via les trips uniques
    stop_pairs = stop_pairs.join(unique_trip_ids, "trip_id", "inner")

    # 5) Joindre avec stops pour récupérer lat/lon
    stops_clean = stops_df.select(
        col("stop_id").cast(StringType()),
        col("stop_lat").cast(FloatType()).alias("lat"),
        col("stop_lon").cast(FloatType()).alias("lon")
    ).filter(is_valid_coord(col("lat"), col("lon")))

    pairs_with_coords = stop_pairs \
        .join(
            stops_clean.withColumnRenamed("stop_id", "dep_stop_id")
                       .withColumnRenamed("lat", "dep_lat")
                       .withColumnRenamed("lon", "dep_lon"),
            "dep_stop_id", "inner"
        ) \
        .join(
            stops_clean.withColumnRenamed("stop_id", "arr_stop_id")
                       .withColumnRenamed("lat", "arr_lat")
                       .withColumnRenamed("lon", "arr_lon"),
            "arr_stop_id", "inner"
        )

    # 6) Calculer distance haversine
    pairs_with_dist = pairs_with_coords.withColumn(
        "distance_km", haversine_col(col("dep_lat"), col("dep_lon"),
                                     col("arr_lat"), col("arr_lon"))
    )

    if pairs_with_dist.count() == 0:
        return None, None

    # 7) Dédupliquer les routes (une seule ligne par paire dep/arr)
    # On garde la distance moyenne par paire
    unique_routes = pairs_with_dist.select(
        col("dep_stop_id").alias("dep_station_id"),
        col("arr_stop_id").alias("arr_station_id"),
        col("distance_km")
    ).groupBy("dep_station_id", "arr_station_id").agg(
        {"distance_km": "avg"}
    ).withColumnRenamed("avg(distance_km)", "distance_km")

    # 8) Préparer les facts : pour chaque paire route, quel type de véhicule ?
    # Joindre pairs_with_dist avec route_type_df pour savoir TGV ou Intercité
    # Détection combinée : route_type 101 OU mots-clés dans le nom de la route
    pairs_with_type = pairs_with_dist.join(
        route_type_df, "route_id", "inner"
    )

    # Construire la condition de détection par mots-clés dans les noms de routes
    # On concatène route_short_name et route_long_name en majuscules
    name_cols = []
    if "route_short_name" in pairs_with_type.columns:
        name_cols.append("route_short_name")
    if "route_long_name" in pairs_with_type.columns:
        name_cols.append("route_long_name")

    if name_cols:
        pairs_with_type = pairs_with_type.withColumn(
            "_route_names_upper",
            upper(concat_ws(" ", *[col(c) for c in name_cols]))
        )
        # route_type 101 OU un des mots-clés trouvé dans le nom
        keyword_condition = lit(False)
        for kw in TGV_KEYWORDS:
            keyword_condition = keyword_condition | col("_route_names_upper").contains(kw)

        pairs_with_type = pairs_with_type.withColumn(
            "vehicle_label",
            when(
                col("route_type").isin(list(TGV_ROUTE_TYPES)) | keyword_condition,
                lit("TGV")
            ).otherwise(lit("Intercité"))
        ).drop("_route_names_upper")
    else:
        # Fallback : uniquement route_type si pas de colonnes de noms
        pairs_with_type = pairs_with_type.withColumn(
            "vehicle_label",
            when(col("route_type").isin(list(TGV_ROUTE_TYPES)), lit("TGV"))
            .otherwise(lit("Intercité"))
        )

    # Faits : paire (dep, arr, vehicle_label) dédupliquée
    facts_raw = pairs_with_type.select(
        col("dep_stop_id").alias("dep_station_id"),
        col("arr_stop_id").alias("arr_station_id"),
        col("vehicle_label"),
        col("distance_km")
    ).groupBy("dep_station_id", "arr_station_id", "vehicle_label").agg(
        {"distance_km": "avg"}
    ).withColumnRenamed("avg(distance_km)", "distance_km")

    # Calculer CO2 par passager
    facts_with_co2 = facts_raw.withColumn(
        "co2_kg_passenger",
        when(col("vehicle_label") == "TGV",
             col("distance_km") * CO2_TGV_PER_100KM / 100.0)
        .otherwise(
             col("distance_km") * CO2_INTERCITE_PER_100KM / 100.0)
    )

    return unique_routes, facts_with_co2


def process_feed_routes(spark, feed_info: dict, station_mapping: dict):
    """Traite les routes et faits d'un feed en utilisant le mapping global de stations."""
    country_code = feed_info["country_code"]
    feed_id = feed_info["feed_id"]
    feed_path = feed_info["path"]

    logger.info(f"  Traitement routes de {country_code}/{feed_id}...")

    routes_df, facts_df = process_train_routes_and_facts(
        spark, feed_path, feed_id, country_code, station_mapping
    )

    if routes_df is None:
        logger.info(f"    Pas de routes train valides, ignoré")
        return

    # Insérer les routes dans dim_route_train
    write_returning_ids(routes_df, f"{SCHEMA}.dim_route_train",
                        "route_train_id", "dep_station_id, arr_station_id")

    # Insérer les faits
    if facts_df is not None and facts_df.count() > 0:
        insert_facts(facts_df)


def insert_facts(facts_df):
    """Insère les faits train dans fact_emission en résolvant les FK depuis PostgreSQL."""
    rows = facts_df.collect()
    if not rows:
        return

    logger.info(f"Insertion de {len(rows)} faits train dans fact_emission...")

    try:
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            temp_file = f.name
            for row in rows:
                dep = str(row["dep_station_id"]).replace("'", "''")
                arr = str(row["arr_station_id"]).replace("'", "''")
                vehicle_label = str(row["vehicle_label"]).replace("'", "''")
                co2 = row["co2_kg_passenger"]

                f.write(f"""INSERT INTO {SCHEMA}.fact_emission (transport_mode, route_train_id, vehicle_train_id, co2_kg_passenger)
SELECT 'train', r.route_train_id, v.vehicle_train_id, {co2}
FROM {SCHEMA}.dim_route_train r, {SCHEMA}.dim_vehicle_train v
WHERE r.dep_station_id = '{dep}'
  AND r.arr_station_id = '{arr}'
  AND v.label = '{vehicle_label}'
ON CONFLICT (route_train_id, vehicle_train_id) DO NOTHING;
""")

        env = os.environ.copy()
        env["PGPASSWORD"] = DB_CONFIG["password"]
        result = subprocess.run(
            [PSQL_PATH, "-h", DB_CONFIG["host"], "-p", DB_CONFIG["port"],
             "-U", DB_CONFIG["user"], "-d", DB_CONFIG["database"],
             "-f", temp_file],
            env=env, capture_output=True, text=True
        )
        os.remove(temp_file)

        if result.returncode == 0:
            logger.info(f"  OK: fact_emission – {len(rows)} faits traités")
        else:
            logger.error(f"  Erreur fact_emission: {result.stderr[:500]}")

    except Exception as e:
        logger.error(f"  Erreur fact_emission: {e}")


def process_backontrack_routes(spark, global_mapping: dict):
    """
    Traite les routes Back on Track (trains de nuit européens).
    Même logique que les GTFS : tronçons consécutifs, distance haversine, insertion routes + faits.
    Tous les véhicules sont classés "Intercité" (trains de nuit).
    """
    trip_stop_path = os.path.join(BACKONTRACK_DIR, "back_on_track_trip_stop.csv")
    stops_path = os.path.join(BACKONTRACK_DIR, "back_on_track_stops.csv")

    if not os.path.exists(trip_stop_path) or not os.path.exists(stops_path):
        logger.warning("Back on Track: fichiers manquants, ignoré")
        return

    logger.info("  Traitement routes Back on Track (trains de nuit)...")

    # Lire trip_stops (≈ stop_times)
    trip_stop_df = spark.read.csv(trip_stop_path, header=True, inferSchema=True)

    # Lire stops pour les coordonnées
    stops_df = spark.read.csv(stops_path, header=True, inferSchema=True)
    stops_df = stops_df.withColumn("lat", col("station_lat").cast(FloatType())) \
                       .withColumn("lon", col("station_long").cast(FloatType()))
    stops_df = stops_df.filter(is_valid_coord(col("lat"), col("lon")))

    # Appliquer le mapping global sur les stop_id
    if global_mapping:
        mapping_data = [(old_id, new_id) for old_id, new_id in global_mapping.items()]
        mapping_df = spark.createDataFrame(mapping_data, ["old_stop_id", "canonical_stop_id"])

        trip_stop_df = trip_stop_df.join(
            broadcast(mapping_df),
            trip_stop_df["stop_id"].cast(StringType()) == mapping_df["old_stop_id"],
            "left"
        ).withColumn(
            "stop_id",
            coalesce(col("canonical_stop_id"), col("stop_id"))
        ).drop("old_stop_id", "canonical_stop_id")

        stops_df = stops_df.join(
            broadcast(mapping_df),
            stops_df["stop_id"].cast(StringType()) == mapping_df["old_stop_id"],
            "left"
        ).withColumn(
            "stop_id",
            coalesce(col("canonical_stop_id"), col("stop_id"))
        ).drop("old_stop_id", "canonical_stop_id")
        stops_df = stops_df.dropDuplicates(["stop_id"])

    # Déduplication des trips par signature (séquence d'arrêts)
    trip_signatures = trip_stop_df.groupBy("trip_id").agg(
        sort_array(collect_list(
            concat(col("stop_sequence").cast(StringType()), lit(":"), col("stop_id").cast(StringType()))
        )).alias("stops_ordered")
    ).withColumn(
        "trip_signature", md5(concat_ws("|", col("stops_ordered")))
    ).drop("stops_ordered")

    trip_signatures = trip_signatures.dropDuplicates(["trip_signature"])
    unique_trip_ids = trip_signatures.select("trip_id")

    # Tronçons consécutifs (arrêt N → arrêt N+1)
    stop_times_unique = trip_stop_df.join(unique_trip_ids, "trip_id", "semi")
    w_trip = Window.partitionBy("trip_id").orderBy(col("stop_sequence").cast(FloatType()))

    stop_pairs = stop_times_unique.withColumn(
        "next_stop_id", lead("stop_id").over(w_trip)
    ).filter(
        col("next_stop_id").isNotNull()
    ).select(
        col("trip_id"),
        col("stop_id").cast(StringType()).alias("dep_stop_id"),
        col("next_stop_id").cast(StringType()).alias("arr_stop_id")
    )

    # Exclure tronçons dep == arr
    stop_pairs = stop_pairs.filter(col("dep_stop_id") != col("arr_stop_id"))

    # Joindre avec stops pour lat/lon
    stops_clean = stops_df.select(
        col("stop_id").cast(StringType()),
        col("lat"), col("lon")
    )

    pairs_with_coords = stop_pairs \
        .join(
            stops_clean.withColumnRenamed("stop_id", "dep_stop_id")
                       .withColumnRenamed("lat", "dep_lat")
                       .withColumnRenamed("lon", "dep_lon"),
            "dep_stop_id", "inner"
        ) \
        .join(
            stops_clean.withColumnRenamed("stop_id", "arr_stop_id")
                       .withColumnRenamed("lat", "arr_lat")
                       .withColumnRenamed("lon", "arr_lon"),
            "arr_stop_id", "inner"
        )

    # Distance haversine
    pairs_with_dist = pairs_with_coords.withColumn(
        "distance_km", haversine_col(col("dep_lat"), col("dep_lon"),
                                     col("arr_lat"), col("arr_lon"))
    )

    if pairs_with_dist.count() == 0:
        logger.info("    Back on Track: aucun tronçon valide")
        return

    # Routes uniques (paire dep/arr avec distance moyenne)
    unique_routes = pairs_with_dist.select(
        col("dep_stop_id").alias("dep_station_id"),
        col("arr_stop_id").alias("arr_station_id"),
        col("distance_km")
    ).groupBy("dep_station_id", "arr_station_id").agg(
        {"distance_km": "avg"}
    ).withColumnRenamed("avg(distance_km)", "distance_km")

    route_count = unique_routes.count()
    logger.info(f"    Back on Track: {route_count} routes uniques")

    # Insérer les routes
    write_returning_ids(unique_routes, f"{SCHEMA}.dim_route_train",
                        "route_train_id", "dep_station_id, arr_station_id")

    # Faits : tous les tronçons = Intercité (trains de nuit)
    facts_raw = pairs_with_dist.select(
        col("dep_stop_id").alias("dep_station_id"),
        col("arr_stop_id").alias("arr_station_id"),
        col("distance_km")
    ).groupBy("dep_station_id", "arr_station_id").agg(
        {"distance_km": "avg"}
    ).withColumnRenamed("avg(distance_km)", "distance_km")

    facts_with_co2 = facts_raw.withColumn(
        "vehicle_label", lit("Intercité")
    ).withColumn(
        "co2_kg_passenger",
        col("distance_km") * CO2_INTERCITE_PER_100KM / 100.0
    )

    if facts_with_co2.count() > 0:
        insert_facts(facts_with_co2)
        logger.info(f"    Back on Track: {facts_with_co2.count()} faits insérés")


def seed_vehicle_tables():
    """Pré-remplit dim_vehicle_train (TGV + Intercité) et dim_vehicle_avion (4 catégories)."""
    logger.info("Seed dim_vehicle_train...")
    run_sql(f"""
        INSERT INTO {SCHEMA}.dim_vehicle_train (label, co2_per_km, service_type) VALUES
            ('TGV', 0.0029, 'grande_vitesse'),
            ('Intercité', 0.0090, 'intercite')
        ON CONFLICT DO NOTHING;
    """)

    logger.info("Seed dim_vehicle_avion...")
    run_sql(f"""
        INSERT INTO {SCHEMA}.dim_vehicle_avion (label, co2_per_km, service_type) VALUES
            ('Avion court-courrier', 0.2250, 'court'),
            ('Avion moyen-courrier', 0.1845, 'moyen'),
            ('Avion moyen-long-courrier', 0.1668, 'moyen_long'),
            ('Avion long-courrier', 0.1779, 'long')
        ON CONFLICT DO NOTHING;
    """)


# ═════════════════════════════════════════════════════════════
#  MAIN — Pipeline en 3 phases
# ═════════════════════════════════════════════════════════════

def main():
    logger.info(f"Démarrage ETL (nouveau schéma) – Logs: {LOG_FILE}")

    # Nettoyage mémoire au démarrage
    gc.collect()
    logger.info("GC initial effectué")

    # Créer le schéma
    ensure_schema()

    # Vider les tables
    if not truncate_tables():
        logger.error("Échec du vidage, arrêt")
        return

    spark = get_spark_session()

    # Télécharger les feeds GTFS (skip ceux déjà présents)
    download_all_gtfs(spark)

    # Pré-remplir les tables de véhicules
    seed_vehicle_tables()

    # Lister les feeds GTFS
    feeds = list_gtfs_feeds(GTFS_DIR)
    logger.info(f"{len(feeds)} feeds GTFS à traiter")

    # ════════════════════════════════════════════════════════
    #  PHASE 1 : Collecte légère des stations de tous les feeds
    # ════════════════════════════════════════════════════════
    logger.info("═══ PHASE 1 : Collecte des stations ═══")
    all_stations = []
    feeds_with_trains = []  # feeds qui ont des arrêts ferroviaires

    for feed in feeds:
        try:
            train_stop_ids = get_train_stop_ids(spark, feed["path"])
            if not train_stop_ids:
                logger.info(f"  {feed['feed_id']}: aucun arrêt ferroviaire, ignoré")
                continue

            feeds_with_trains.append(feed)

            stations = collect_stations_from_feed(
                spark, feed["path"], feed["country_code"], train_stop_ids
            )
            all_stations.extend(stations)
            logger.info(f"  {feed['feed_id']}: {len(stations)} stations collectées")

            spark.catalog.clearCache()
        except Exception as e:
            logger.error(f"  Erreur collecte {feed['feed_id']}: {e}")
            import traceback
            logger.error(traceback.format_exc())

    # Collecter les stations Back on Track (trains de nuit)
    try:
        bot_stations = collect_stations_from_backontrack(spark)
        all_stations.extend(bot_stations)
        logger.info(f"  Back on Track: {len(bot_stations)} stations collectées")
        spark.catalog.clearCache()
    except Exception as e:
        logger.error(f"  Erreur collecte Back on Track: {e}")
        import traceback
        logger.error(traceback.format_exc())

    if not all_stations:
        logger.error("Aucune station collectée, arrêt")
        spark.catalog.clearCache()
        spark.stop()
        gc.collect()
        return

    logger.info(f"Phase 1 terminée: {len(all_stations)} stations de {len(feeds_with_trains)} feeds + Back on Track")

    # ════════════════════════════════════════════════════════
    #  PHASE 2 : Déduplication globale et insertion dim_station
    # ════════════════════════════════════════════════════════
    logger.info("═══ PHASE 2 : Déduplication globale ═══")
    canonical_stations, global_mapping = build_global_station_registry(all_stations)

    # Insérer les stations canoniques dans dim_station
    if canonical_stations:
        stations_df = spark.createDataFrame(canonical_stations)
        write_to_postgres_upsert(stations_df, f"{SCHEMA}.dim_station", "station_id")
        del stations_df

    logger.info(f"Phase 2 terminée: {len(canonical_stations)} stations canoniques, {len(global_mapping)} remappings")

    # Libérer la mémoire des stations collectées
    del all_stations
    gc.collect()

    # ════════════════════════════════════════════════════════
    #  PHASE 3 : Traitement des routes et faits par feed
    # ════════════════════════════════════════════════════════
    logger.info("═══ PHASE 3 : Routes et faits ═══")
    for feed in feeds_with_trains:
        try:
            # Construire le mapping complet pour ce feed
            feed_mapping = build_feed_stop_id_mapping(spark, feed["path"], global_mapping)
            logger.info(f"  {feed['feed_id']}: {len(feed_mapping)} stop_ids remappés")

            # Traiter les routes et faits
            process_feed_routes(spark, feed, feed_mapping)

            # Libérer la mémoire Spark entre les feeds
            spark.catalog.clearCache()
            gc.collect()
        except Exception as e:
            logger.error(f"  Erreur {feed['feed_id']}: {e}")
            import traceback
            logger.error(traceback.format_exc())

    # Traiter les routes Back on Track (trains de nuit)
    try:
        process_backontrack_routes(spark, global_mapping)
        spark.catalog.clearCache()
        gc.collect()
    except Exception as e:
        logger.error(f"  Erreur Back on Track routes: {e}")
        import traceback
        logger.error(traceback.format_exc())

    # Nettoyage mémoire final
    spark.catalog.clearCache()
    logger.info("Cache Spark vidé")
    spark.stop()
    gc.collect()
    logger.info("GC final effectué")
    logger.info("Terminé!")


if __name__ == "__main__":
    main()
