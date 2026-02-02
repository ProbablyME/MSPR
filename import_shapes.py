"""
Importe les shapes GTFS dans PostgreSQL pour visualisation dans QGIS.
Uniquement pour les trajets de train (ceux présents dans la table trajet).
"""

import os
import csv
import psycopg2
from collections import defaultdict

DB_CONFIG = {
    "host": "localhost",
    "port": "5432",
    "database": "postgres",
    "user": "postgres",
    "password": "lpironti"
}

GTFS_DIR = "downloads_gtfs"

# Types de routes pour les trains (standard GTFS + codes étendus)
TRAIN_ROUTE_TYPES = {2} | set(range(100, 118))


def create_table(conn):
    """Crée la table shapes_trips."""
    with conn.cursor() as cur:
        cur.execute("""
            DROP TABLE IF EXISTS shapes_trips CASCADE;
            CREATE TABLE shapes_trips (
                id SERIAL PRIMARY KEY,
                shape_id VARCHAR(255) UNIQUE,
                feed_id VARCHAR(100),
                country VARCHAR(10),
                geom GEOMETRY(LINESTRING, 4326)
            );
            CREATE INDEX idx_shapes_trips_geom ON shapes_trips USING GIST(geom);
        """)
    conn.commit()
    print("Table shapes_trips créée")


def get_train_shape_ids(feed_path: str) -> set:
    """Récupère les shape_id des trajets de train uniquement."""
    routes_file = os.path.join(feed_path, "routes.txt")
    trips_file = os.path.join(feed_path, "trips.txt")

    if not os.path.exists(routes_file) or not os.path.exists(trips_file):
        return set()

    # 1. Trouver les route_id de type train
    train_routes = set()
    with open(routes_file, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                route_type = int(row.get("route_type", 0))
                if route_type in TRAIN_ROUTE_TYPES:
                    train_routes.add(row.get("route_id"))
            except ValueError:
                continue

    if not train_routes:
        return set()

    # 2. Trouver les shape_id des trips de ces routes
    shape_ids = set()
    with open(trips_file, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            route_id = row.get("route_id")
            shape_id = row.get("shape_id")
            if route_id in train_routes and shape_id:
                shape_ids.add(shape_id)

    return shape_ids


def find_feeds_with_shapes():
    """Trouve tous les feeds avec shapes.txt."""
    feeds = []

    for country_code in os.listdir(GTFS_DIR):
        country_path = os.path.join(GTFS_DIR, country_code)
        if not os.path.isdir(country_path):
            continue

        for feed_id in os.listdir(country_path):
            feed_path = os.path.join(country_path, feed_id)
            shapes_file = os.path.join(feed_path, "shapes.txt")

            if os.path.exists(shapes_file) and os.path.getsize(shapes_file) > 50:
                feeds.append({
                    "country": country_code,
                    "feed_id": feed_id,
                    "feed_path": feed_path,
                    "shapes_file": shapes_file
                })

    return feeds


def parse_shapes_file(shapes_file: str, valid_shape_ids: set) -> dict:
    """Parse un fichier shapes.txt, ne garde que les shape_id valides."""
    shapes = defaultdict(list)

    with open(shapes_file, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)

        for row in reader:
            shape_id = row.get("shape_id")

            # Filtrer: ne garder que les shapes de train
            if shape_id not in valid_shape_ids:
                continue

            lat = row.get("shape_pt_lat")
            lon = row.get("shape_pt_lon")
            seq = row.get("shape_pt_sequence")

            if lat and lon and seq:
                try:
                    shapes[shape_id].append({
                        "lat": float(lat),
                        "lon": float(lon),
                        "seq": int(seq)
                    })
                except ValueError:
                    continue

    return shapes


def shapes_to_geometries(shapes: dict) -> list:
    """Convertit les shapes en géométries WKT."""
    geometries = []

    for shape_id, points in shapes.items():
        if len(points) < 2:
            continue

        # Trier par sequence
        points.sort(key=lambda p: p["seq"])

        # Construire WKT
        coords = ",".join([f"{p['lon']} {p['lat']}" for p in points])
        wkt = f"SRID=4326;LINESTRING({coords})"

        geometries.append((shape_id, wkt))

    return geometries


def import_shapes(conn):
    """Importe les shapes des trains uniquement."""
    feeds = find_feeds_with_shapes()
    print(f"Trouvé {len(feeds)} feeds avec shapes.txt")

    total = 0

    for info in feeds:
        # Récupérer les shape_id des trains uniquement
        train_shape_ids = get_train_shape_ids(info["feed_path"])

        if not train_shape_ids:
            continue

        print(f"  {info['country']}/{info['feed_id']}: {len(train_shape_ids)} shapes de train...", end=" ")

        shapes = parse_shapes_file(info["shapes_file"], train_shape_ids)
        geometries = shapes_to_geometries(shapes)

        if not geometries:
            print("0 importés")
            continue

        with conn.cursor() as cur:
            for shape_id, wkt in geometries:
                try:
                    cur.execute("""
                        INSERT INTO shapes_trips (shape_id, feed_id, country, geom)
                        VALUES (%s, %s, %s, ST_GeomFromEWKT(%s))
                        ON CONFLICT (shape_id) DO NOTHING
                    """, (shape_id, info["feed_id"], info["country"], wkt))
                except Exception:
                    pass

        conn.commit()
        print(f"{len(geometries)} importés")
        total += len(geometries)

    return total


def main():
    conn = psycopg2.connect(**DB_CONFIG)

    try:
        create_table(conn)
        total = import_shapes(conn)

        print(f"\n=== TOTAL: {total} shapes importés ===")

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM shapes_trips")
            count = cur.fetchone()[0]

            cur.execute("""
                SELECT country, COUNT(*)
                FROM shapes_trips
                GROUP BY country
                ORDER BY COUNT(*) DESC
            """)
            print("\nPar pays:")
            for row in cur.fetchall():
                print(f"  {row[0]}: {row[1]}")

    finally:
        conn.close()

    print("\n--- Dans QGIS ---")
    print("Ajoutez la couche 'shapes_trips' pour voir les tracés GTFS")


if __name__ == "__main__":
    main()
