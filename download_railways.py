"""
Télécharge les voies ferrées depuis Geofabrik (OSM) pour n'importe quel pays européen
et les importe dans PostgreSQL.

Utilise osmium pour filtrer uniquement les railways du fichier PBF.
"""

import subprocess
import os
import json
import psycopg2

DB_CONFIG = {
    "host": "localhost",
    "port": "5432",
    "database": "postgres",
    "user": "postgres",
    "password": "lpironti"
}

# URLs Geofabrik par pays (format PBF, ~100-500 Mo par pays)
GEOFABRIK_URLS = {
    "france": "https://download.geofabrik.de/europe/france-latest.osm.pbf",
    "germany": "https://download.geofabrik.de/europe/germany-latest.osm.pbf",
    "spain": "https://download.geofabrik.de/europe/spain-latest.osm.pbf",
    "italy": "https://download.geofabrik.de/europe/italy-latest.osm.pbf",
    "belgium": "https://download.geofabrik.de/europe/belgium-latest.osm.pbf",
    "netherlands": "https://download.geofabrik.de/europe/netherlands-latest.osm.pbf",
    "switzerland": "https://download.geofabrik.de/europe/switzerland-latest.osm.pbf",
    "austria": "https://download.geofabrik.de/europe/austria-latest.osm.pbf",
    "poland": "https://download.geofabrik.de/europe/poland-latest.osm.pbf",
    "czech-republic": "https://download.geofabrik.de/europe/czech-republic-latest.osm.pbf",
    "united-kingdom": "https://download.geofabrik.de/europe/great-britain-latest.osm.pbf",
    "portugal": "https://download.geofabrik.de/europe/portugal-latest.osm.pbf",
    "sweden": "https://download.geofabrik.de/europe/sweden-latest.osm.pbf",
    "norway": "https://download.geofabrik.de/europe/norway-latest.osm.pbf",
    "denmark": "https://download.geofabrik.de/europe/denmark-latest.osm.pbf",
    "finland": "https://download.geofabrik.de/europe/finland-latest.osm.pbf",
    "ireland": "https://download.geofabrik.de/europe/ireland-and-northern-ireland-latest.osm.pbf",
    "greece": "https://download.geofabrik.de/europe/greece-latest.osm.pbf",
    "hungary": "https://download.geofabrik.de/europe/hungary-latest.osm.pbf",
    "romania": "https://download.geofabrik.de/europe/romania-latest.osm.pbf",
    "bulgaria": "https://download.geofabrik.de/europe/bulgaria-latest.osm.pbf",
    "croatia": "https://download.geofabrik.de/europe/croatia-latest.osm.pbf",
    "slovakia": "https://download.geofabrik.de/europe/slovakia-latest.osm.pbf",
    "slovenia": "https://download.geofabrik.de/europe/slovenia-latest.osm.pbf",
    "luxembourg": "https://download.geofabrik.de/europe/luxembourg-latest.osm.pbf",
}

DATA_DIR = "osm_data"


def download_country_pbf(country: str) -> str:
    """Télécharge le fichier PBF d'un pays depuis Geofabrik."""
    if country not in GEOFABRIK_URLS:
        raise ValueError(f"Pays non supporté: {country}. Disponibles: {list(GEOFABRIK_URLS.keys())}")

    os.makedirs(DATA_DIR, exist_ok=True)
    pbf_file = os.path.join(DATA_DIR, f"{country}-latest.osm.pbf")

    if os.path.exists(pbf_file):
        print(f"  Fichier {pbf_file} déjà présent, skip téléchargement")
        return pbf_file

    url = GEOFABRIK_URLS[country]
    print(f"  Téléchargement de {country} depuis Geofabrik...")
    print(f"  URL: {url}")

    subprocess.run([
        "curl", "-L", "-o", pbf_file, url
    ], check=True)

    print(f"  Téléchargé: {pbf_file}")
    return pbf_file


def extract_railways(pbf_file: str, country: str) -> str:
    """Extrait uniquement les railways du fichier PBF avec osmium."""
    railways_pbf = os.path.join(DATA_DIR, f"{country}-railways.osm.pbf")

    if os.path.exists(railways_pbf):
        print(f"  Fichier {railways_pbf} déjà présent, skip extraction")
        return railways_pbf

    print(f"  Extraction des railways avec osmium...")

    subprocess.run([
        "osmium", "tags-filter",
        pbf_file,
        "w/railway=rail",  # w = ways uniquement, railway=rail = voies ferrées principales
        "-o", railways_pbf,
        "--overwrite"
    ], check=True)

    print(f"  Extrait: {railways_pbf}")
    return railways_pbf


def convert_to_geojson(railways_pbf: str, country: str) -> str:
    """Convertit le PBF en GeoJSON."""
    geojson_file = os.path.join(DATA_DIR, f"{country}-railways.geojson")

    if os.path.exists(geojson_file):
        print(f"  Fichier {geojson_file} déjà présent, skip conversion")
        return geojson_file

    print(f"  Conversion en GeoJSON...")

    subprocess.run([
        "osmium", "export",
        railways_pbf,
        "-o", geojson_file,
        "--overwrite",
        "-f", "geojson"
    ], check=True)

    print(f"  Converti: {geojson_file}")
    return geojson_file


def create_table(conn):
    """Crée la table pour stocker les voies ferrées OSM."""
    with conn.cursor() as cur:
        cur.execute("""
            DROP TABLE IF EXISTS osm_railways CASCADE;
            CREATE TABLE osm_railways (
                id SERIAL PRIMARY KEY,
                osm_id BIGINT,
                name VARCHAR(255),
                country VARCHAR(50),
                railway_type VARCHAR(50),
                electrified VARCHAR(50),
                usage VARCHAR(50),
                maxspeed VARCHAR(20),
                geom GEOMETRY(LINESTRING, 4326)
            );
            CREATE INDEX idx_osm_railways_geom ON osm_railways USING GIST(geom);
            CREATE INDEX idx_osm_railways_country ON osm_railways(country);
        """)
    conn.commit()
    print("Table osm_railways créée")


def import_geojson_to_postgres(geojson_file: str, country: str, conn):
    """Importe le GeoJSON dans PostgreSQL."""
    print(f"  Import de {geojson_file} dans PostgreSQL...")

    with open(geojson_file, "r") as f:
        data = json.load(f)

    features = data.get("features", [])
    print(f"  {len(features)} features à importer")

    railways = []
    for feature in features:
        geom_type = feature.get("geometry", {}).get("type")
        if geom_type != "LineString":
            continue

        coords = feature["geometry"]["coordinates"]
        if len(coords) < 2:
            continue

        # Construire WKT
        wkt_coords = ",".join([f"{c[0]} {c[1]}" for c in coords])
        wkt = f"SRID=4326;LINESTRING({wkt_coords})"

        props = feature.get("properties", {})

        railways.append((
            props.get("@id", "").replace("way/", "") or None,
            props.get("name"),
            country,
            props.get("railway", "rail"),
            props.get("electrified"),
            props.get("usage"),
            props.get("maxspeed"),
            wkt
        ))

    with conn.cursor() as cur:
        batch_size = 1000
        for i in range(0, len(railways), batch_size):
            batch = railways[i:i+batch_size]
            cur.executemany("""
                INSERT INTO osm_railways (osm_id, name, country, railway_type, electrified, usage, maxspeed, geom)
                VALUES (%s, %s, %s, %s, %s, %s, %s, ST_GeomFromEWKT(%s))
            """, batch)
            print(f"    {min(i+batch_size, len(railways))}/{len(railways)}")

    conn.commit()
    print(f"  Import terminé: {len(railways)} voies ferrées")
    return len(railways)


def process_country(country: str, conn):
    """Traite un pays complet: téléchargement, extraction, import."""
    print(f"\n=== Traitement de {country.upper()} ===")

    pbf_file = download_country_pbf(country)
    railways_pbf = extract_railways(pbf_file, country)
    geojson_file = convert_to_geojson(railways_pbf, country)
    count = import_geojson_to_postgres(geojson_file, country, conn)

    return count


def main():
    import sys

    # Par défaut: France. Sinon prendre les arguments
    if len(sys.argv) > 1:
        countries = sys.argv[1:]
    else:
        countries = ["france"]

    # Valider les pays
    for c in countries:
        if c not in GEOFABRIK_URLS:
            print(f"Pays non supporté: {c}")
            print(f"Pays disponibles: {', '.join(sorted(GEOFABRIK_URLS.keys()))}")
            return

    conn = psycopg2.connect(**DB_CONFIG)

    try:
        create_table(conn)

        total = 0
        for country in countries:
            count = process_country(country, conn)
            total += count

        print(f"\n=== TOTAL: {total} voies ferrées importées ===")

        # Stats par pays
        with conn.cursor() as cur:
            cur.execute("""
                SELECT country, COUNT(*)
                FROM osm_railways
                GROUP BY country
            """)
            print("\nPar pays:")
            for row in cur.fetchall():
                print(f"  {row[0]}: {row[1]}")

    finally:
        conn.close()

    print("\n--- Dans QGIS ---")
    print("1. Ajoutez la couche 'osm_railways' (voies ferrées OSM)")
    print("2. Ajoutez la couche 'trajet_geom' (vos trajets GTFS)")
    print("3. Superposez pour voir la couverture")


if __name__ == "__main__":
    main()
