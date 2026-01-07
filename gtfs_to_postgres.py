from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, concat_ws, max as spark_max
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, FloatType, BooleanType, IntegerType
import os
import sys
import logging
from datetime import datetime

# Configuration du logging vers fichier
LOG_FILE = f"gtfs_import_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Configuration PostgreSQL
DB_CONFIG = {
    "host": "localhost",
    "port": "5432",
    "database": "postgres",
    "user": "postgres",
    "password": "lpironti"
}

JDBC_URL = f"jdbc:postgresql://{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
JDBC_PROPERTIES = {
    "user": DB_CONFIG["user"],
    "password": DB_CONFIG["password"],
    "driver": "org.postgresql.Driver"
}

# Dossier des GTFS téléchargés
GTFS_DIR = "downloads_gtfs"

# Tables à vider avant import (ordre respectant les FK)
TABLES_TO_TRUNCATE = [
    "arret_horaire",
    "trajet",
    "ligne",
    "station",
    "operateur"
]

# Types de routes pour les trains (standard GTFS + codes étendus)
TRAIN_ROUTE_TYPES = {2} | set(range(100, 118))


def get_spark_session():
    """Crée une session Spark avec le driver PostgreSQL."""
    return SparkSession.builder \
        .appName("GTFS to PostgreSQL") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()


def run_sql(sql_command: str) -> bool:
    """Exécute une commande SQL via psql."""
    import subprocess

    env = os.environ.copy()
    env["PGPASSWORD"] = DB_CONFIG["password"]

    result = subprocess.run(
        [
            "/Library/PostgreSQL/18/bin/psql",
            "-h", DB_CONFIG["host"],
            "-p", DB_CONFIG["port"],
            "-U", DB_CONFIG["user"],
            "-d", DB_CONFIG["database"],
            "-c", sql_command
        ],
        env=env,
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        logger.error(f"Erreur psql: {result.stderr}")
        return False
    return True


def truncate_tables():
    """Vide les tables GTFS avant import (respecte l'ordre des FK)."""
    import subprocess

    logger.info("Vidage des tables GTFS...")

    try:
        # Construire la commande SQL
        tables_list = ", ".join(TABLES_TO_TRUNCATE)
        sql_command = f"TRUNCATE TABLE {tables_list} CASCADE;"

        logger.info(f"  Exécution: {sql_command}")

        # Utiliser psql via subprocess
        env = os.environ.copy()
        env["PGPASSWORD"] = DB_CONFIG["password"]

        result = subprocess.run(
            [
                "/Library/PostgreSQL/18/bin/psql",
                "-h", DB_CONFIG["host"],
                "-p", DB_CONFIG["port"],
                "-U", DB_CONFIG["user"],
                "-d", DB_CONFIG["database"],
                "-c", sql_command
            ],
            env=env,
            capture_output=True,
            text=True
        )

        if result.returncode == 0:
            logger.info("Tables vidées avec succès")
            return True
        else:
            logger.error(f"Erreur psql: {result.stderr}")
            return False

    except FileNotFoundError:
        logger.error("psql non trouvé. Installez PostgreSQL ou ajoutez psql au PATH")
        return False
    except Exception as e:
        logger.error(f"Erreur lors du vidage des tables: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


def disable_fk_constraints():
    """Désactive les contraintes FK sur arret_horaire."""
    logger.info("Désactivation des contraintes FK sur arret_horaire...")
    run_sql("ALTER TABLE arret_horaire DROP CONSTRAINT IF EXISTS fk_arret_station;")
    run_sql("ALTER TABLE arret_horaire DROP CONSTRAINT IF EXISTS fk_arret_trajet;")
    logger.info("Contraintes FK désactivées")


def enable_fk_constraints():
    """Réactive les contraintes FK sur arret_horaire (sans valider les données existantes)."""
    logger.info("Réactivation des contraintes FK sur arret_horaire...")
    # NOT VALID permet d'ajouter la contrainte sans valider les données existantes
    run_sql("ALTER TABLE arret_horaire ADD CONSTRAINT fk_arret_station FOREIGN KEY (id_station) REFERENCES station(id_station) NOT VALID;")
    run_sql("ALTER TABLE arret_horaire ADD CONSTRAINT fk_arret_trajet FOREIGN KEY (id_trajet) REFERENCES trajet(id_trajet) NOT VALID;")
    logger.info("Contraintes FK réactivées")


def list_gtfs_feeds(base_dir: str) -> list:
    """Liste tous les dossiers GTFS disponibles."""
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
    """Lit un fichier GTFS CSV s'il existe."""
    filepath = os.path.join(feed_path, filename)
    if os.path.exists(filepath):
        return spark.read.csv(filepath, header=True, inferSchema=True)
    return None


def process_agencies(spark, feed_path: str, country_code: str):
    """Traite agency.txt -> Table Operateur."""
    df = read_gtfs_file(spark, feed_path, "agency.txt")
    if df is None:
        return None

    return df.select(
        col("agency_id").cast(StringType()).alias("id_operateur"),
        col("agency_name").cast(StringType()).alias("nom_operateur"),
        lit(country_code).alias("code_pays")
    ).dropDuplicates(["id_operateur"])


def process_stops(spark, feed_path: str, country_code: str):
    """Traite stops.txt -> Table Station."""
    df = read_gtfs_file(spark, feed_path, "stops.txt")
    if df is None:
        return None

    # Filtrer uniquement les stations (location_type = 0 ou 1, ou null)
    if "location_type" in df.columns:
        df = df.filter((col("location_type").isNull()) | (col("location_type").isin([0, 1])))

    result = df.select(
        col("stop_id").cast(StringType()).alias("id_station"),
        col("stop_name").cast(StringType()).alias("nom_station"),
        col("stop_name").cast(StringType()).alias("ville"),  # Approximation
        lit(country_code).alias("code_pays"),
        col("stop_lat").cast(FloatType()).alias("latitude"),
        col("stop_lon").cast(FloatType()).alias("longitude"),
        lit(False).alias("est_aeroport")
    ).dropDuplicates(["id_station"])

    return result


def process_routes(spark, feed_path: str, feed_id: str):
    """Traite routes.txt -> Table Ligne (uniquement les trains)."""
    df = read_gtfs_file(spark, feed_path, "routes.txt")
    if df is None:
        return None

    # Filtrer uniquement les lignes de train (cast en int pour gérer les deux cas)
    df = df.filter(col("route_type").cast(IntegerType()).isin(list(TRAIN_ROUTE_TYPES)))

    if df.count() == 0:
        return None

    # Utiliser route_long_name ou route_short_name si null
    nom_ligne_col = col("route_long_name")
    if "route_short_name" in df.columns:
        nom_ligne_col = when(
            col("route_long_name").isNull(),
            col("route_short_name")
        ).otherwise(col("route_long_name"))

    result = df.select(
        col("route_id").cast(StringType()).alias("id_ligne"),
        nom_ligne_col.cast(StringType()).alias("nom_ligne"),
        col("route_type").cast(StringType()).alias("type_ligne"),
        col("agency_id").cast(StringType()).alias("id_operateur")
    )

    return result.dropDuplicates(["id_ligne"])


def process_trips(spark, feed_path: str, train_route_ids: list):
    """Traite trips.txt -> Table Trajet (uniquement pour les routes de train)."""
    trips_df = read_gtfs_file(spark, feed_path, "trips.txt")
    if trips_df is None or not train_route_ids:
        return None

    # Filtrer uniquement les trajets des lignes de train
    trips_df = trips_df.filter(col("route_id").isin(train_route_ids))

    if trips_df.count() == 0:
        return None

    # Charger stop_times et stops pour trouver la dernière station de chaque trajet
    stop_times_df = read_gtfs_file(spark, feed_path, "stop_times.txt")
    stops_df = read_gtfs_file(spark, feed_path, "stops.txt")

    if stop_times_df is not None and stops_df is not None:
        # Trouver le dernier arrêt de chaque trajet (max stop_sequence)
        window = Window.partitionBy("trip_id")
        last_stops = stop_times_df.withColumn(
            "max_seq", spark_max("stop_sequence").over(window)
        ).filter(
            col("stop_sequence") == col("max_seq")
        ).select(
            col("trip_id"),
            col("stop_id").alias("last_stop_id")
        ).dropDuplicates(["trip_id"])

        # Joindre avec stops pour avoir le nom de la station
        last_stops_with_names = last_stops.join(
            stops_df.select(
                col("stop_id"),
                col("stop_name").alias("direction")
            ),
            last_stops.last_stop_id == stops_df.stop_id,
            "left"
        ).select("trip_id", "direction")

        # Joindre avec trips
        result = trips_df.join(
            last_stops_with_names,
            "trip_id",
            "left"
        ).select(
            col("trip_id").cast(StringType()).alias("id_trajet"),
            col("route_id").cast(StringType()).alias("id_ligne"),
            lit(False).alias("est_train_nuit"),
            col("direction").cast(StringType()).alias("direction"),
            lit(1).alias("frequence"),
            lit(False).alias("est_avion")
        )
    else:
        # Fallback: utiliser trip_headsign si stop_times/stops non disponibles
        result = trips_df.select(
            col("trip_id").cast(StringType()).alias("id_trajet"),
            col("route_id").cast(StringType()).alias("id_ligne"),
            lit(False).alias("est_train_nuit"),
            col("trip_headsign").cast(StringType()).alias("direction"),
            lit(1).alias("frequence"),
            lit(False).alias("est_avion")
        )

    return result.dropDuplicates(["id_trajet"])


def process_stop_times(spark, feed_path: str, trips_df):
    """Traite stop_times.txt -> Table Arret_Horaire."""
    df = read_gtfs_file(spark, feed_path, "stop_times.txt")
    if df is None or trips_df is None:
        return None

    # Filtrer via join (plus efficace que isin avec 38000+ IDs)
    valid_trips = trips_df.select(col("id_trajet").alias("trip_id")).distinct()
    df = df.join(valid_trips, "trip_id", "inner")

    # Gérer shape_dist_traveled si disponible
    if "shape_dist_traveled" in df.columns:
        result = df.select(
            col("trip_id").cast(StringType()).alias("id_trajet"),
            col("stop_id").cast(StringType()).alias("id_station"),
            col("departure_time").cast(StringType()).alias("heure_depart"),
            col("arrival_time").cast(StringType()).alias("heure_arrivee"),
            col("stop_sequence").cast(IntegerType()).alias("sequence_arret"),
            (col("shape_dist_traveled").cast(FloatType()) / 1000).alias("distance_parcourue_km")
        )
    else:
        result = df.select(
            col("trip_id").cast(StringType()).alias("id_trajet"),
            col("stop_id").cast(StringType()).alias("id_station"),
            col("departure_time").cast(StringType()).alias("heure_depart"),
            col("arrival_time").cast(StringType()).alias("heure_arrivee"),
            col("stop_sequence").cast(IntegerType()).alias("sequence_arret"),
            lit(None).cast(FloatType()).alias("distance_parcourue_km")
        )

    return result


def write_to_postgres_upsert(df, table_name: str, primary_key: str):
    """Écrit un DataFrame dans PostgreSQL avec gestion des doublons (INSERT ON CONFLICT DO NOTHING)."""
    import subprocess
    import tempfile

    if df is None:
        logger.warning(f"Aucune donnée pour {table_name} (DataFrame None)")
        return 0

    count = df.count()
    if count == 0:
        logger.warning(f"Aucune donnée pour {table_name} (0 lignes)")
        return 0

    logger.info(f"Tentative d'écriture de {count} lignes dans {table_name}...")

    try:
        # Collecter les données et les insérer via psql avec ON CONFLICT DO NOTHING
        rows = df.collect()
        columns = df.columns

        # Créer un fichier temporaire avec les commandes SQL
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            temp_file = f.name
            for row in rows:
                values = []
                for col in columns:
                    val = row[col]
                    if val is None:
                        values.append("NULL")
                    elif isinstance(val, (int, float)):
                        values.append(str(val))
                    elif isinstance(val, bool):
                        values.append("TRUE" if val else "FALSE")
                    else:
                        # Échapper les apostrophes
                        val_str = str(val).replace("'", "''")
                        values.append(f"'{val_str}'")

                cols_str = ", ".join(f'"{c}"' for c in columns)
                vals_str = ", ".join(values)
                f.write(f"INSERT INTO {table_name} ({cols_str}) VALUES ({vals_str}) ON CONFLICT ({primary_key}) DO NOTHING;\n")

        # Exécuter via psql
        env = os.environ.copy()
        env["PGPASSWORD"] = DB_CONFIG["password"]

        result = subprocess.run(
            [
                "/Library/PostgreSQL/18/bin/psql",
                "-h", DB_CONFIG["host"],
                "-p", DB_CONFIG["port"],
                "-U", DB_CONFIG["user"],
                "-d", DB_CONFIG["database"],
                "-f", temp_file
            ],
            env=env,
            capture_output=True,
            text=True
        )

        # Nettoyer le fichier temporaire
        os.remove(temp_file)

        if result.returncode == 0:
            logger.info(f"SUCCESS: {table_name}: {count} lignes traitées (doublons ignorés)")
            return count
        else:
            logger.error(f"Erreur psql: {result.stderr}")
            return 0

    except Exception as e:
        logger.error(f"ERREUR lors de l'écriture dans {table_name}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 0


def write_to_postgres_simple(df, table_name: str):
    """Écrit un DataFrame dans PostgreSQL avec INSERT simple (sans ON CONFLICT)."""
    import subprocess
    import tempfile

    if df is None:
        logger.warning(f"Aucune donnée pour {table_name} (DataFrame None)")
        return 0

    count = df.count()
    if count == 0:
        logger.warning(f"Aucune donnée pour {table_name} (0 lignes)")
        return 0

    logger.info(f"Tentative d'écriture de {count} lignes dans {table_name}...")

    try:
        # Collecter les données et les insérer via psql
        rows = df.collect()
        columns = df.columns

        # Créer un fichier temporaire avec les commandes SQL
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            temp_file = f.name
            for row in rows:
                values = []
                for col in columns:
                    val = row[col]
                    if val is None:
                        values.append("NULL")
                    elif isinstance(val, (int, float)):
                        values.append(str(val))
                    elif isinstance(val, bool):
                        values.append("TRUE" if val else "FALSE")
                    else:
                        # Échapper les apostrophes
                        val_str = str(val).replace("'", "''")
                        values.append(f"'{val_str}'")

                cols_str = ", ".join(f'"{c}"' for c in columns)
                vals_str = ", ".join(values)
                f.write(f"INSERT INTO {table_name} ({cols_str}) VALUES ({vals_str});\n")

        # Exécuter via psql
        env = os.environ.copy()
        env["PGPASSWORD"] = DB_CONFIG["password"]

        result = subprocess.run(
            [
                "/Library/PostgreSQL/18/bin/psql",
                "-h", DB_CONFIG["host"],
                "-p", DB_CONFIG["port"],
                "-U", DB_CONFIG["user"],
                "-d", DB_CONFIG["database"],
                "-f", temp_file
            ],
            env=env,
            capture_output=True,
            text=True
        )

        # Nettoyer le fichier temporaire
        os.remove(temp_file)

        if result.returncode == 0:
            logger.info(f"SUCCESS: {table_name}: {count} lignes insérées")
            return count
        else:
            logger.error(f"Erreur psql: {result.stderr}")
            return 0

    except Exception as e:
        logger.error(f"ERREUR lors de l'écriture dans {table_name}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 0


def write_to_postgres(df, table_name: str, mode: str = "append"):
    """Écrit un DataFrame dans PostgreSQL."""
    if df is None:
        logger.warning(f"Aucune donnée pour {table_name} (DataFrame None)")
        return 0

    count = df.count()
    if count == 0:
        logger.warning(f"Aucune donnée pour {table_name} (0 lignes)")
        return 0

    logger.info(f"Tentative d'écriture de {count} lignes dans {table_name}...")
    logger.info(f"Schéma: {df.schema.simpleString()}")
    logger.info(f"Aperçu des données:")
    df.show(3, truncate=False)

    try:
        df.write.jdbc(
            url=JDBC_URL,
            table=table_name,
            mode=mode,
            properties=JDBC_PROPERTIES
        )
        logger.info(f"SUCCESS: {table_name}: {count} lignes insérées")
        return count
    except Exception as e:
        logger.error(f"ERREUR lors de l'écriture dans {table_name}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 0


def process_feed(spark, feed_info: dict):
    """Traite un feed GTFS complet."""
    country_code = feed_info["country_code"]
    feed_id = feed_info["feed_id"]
    feed_path = feed_info["path"]

    logger.info(f"Traitement de {country_code}/{feed_id}...")

    # 1. Vérifier s'il y a des lignes de train
    routes_df = process_routes(spark, feed_path, feed_id)
    if routes_df is None:
        logger.warning(f"Pas de lignes de train, ignoré")
        return

    train_route_ids = [row.id_ligne for row in routes_df.select("id_ligne").collect()]

    # 2. Traiter les opérateurs (avec gestion des doublons)
    agencies_df = process_agencies(spark, feed_path, country_code)
    write_to_postgres_upsert(agencies_df, "operateur", "id_operateur")

    # 3. Traiter les stations (avec gestion des doublons)
    stops_df = process_stops(spark, feed_path, country_code)
    write_to_postgres_upsert(stops_df, "station", "id_station")

    # 4. Écrire les lignes (avec gestion des doublons)
    write_to_postgres_upsert(routes_df, "ligne", "id_ligne")

    # 5. Traiter les trajets (avec gestion des doublons)
    trips_df = process_trips(spark, feed_path, train_route_ids)
    if trips_df is not None:
        write_to_postgres_upsert(trips_df, "trajet", "id_trajet")

        # 6. Traiter les arrêts horaires (insertion simple, pas de ON CONFLICT car id_arret est auto-increment)
        stop_times_df = process_stop_times(spark, feed_path, trips_df)
        write_to_postgres_simple(stop_times_df, "arret_horaire")


def main():
    logger.info(f"Démarrage - Logs enregistrés dans {LOG_FILE}")

    # Vider les tables GTFS avant import
    if not truncate_tables():
        logger.error("Échec du vidage des tables, arrêt du script")
        return

    # Désactiver les contraintes FK pour permettre l'import
    disable_fk_constraints()

    spark = get_spark_session()

    # Lister tous les feeds GTFS disponibles
    feeds = list_gtfs_feeds(GTFS_DIR)

    logger.info(f"Nombre de feeds GTFS à traiter: {len(feeds)}")

    # Traiter chaque feed
    for feed in feeds:
        try:
            process_feed(spark, feed)
        except Exception as e:
            logger.error(f"Erreur lors du traitement de {feed['feed_id']}: {e}")
            import traceback
            logger.error(traceback.format_exc())

    # Réactiver les contraintes FK
    enable_fk_constraints()

    logger.info("Terminé!")
    spark.stop()


if __name__ == "__main__":
    main()
