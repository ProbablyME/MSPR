from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import requests
import os
import zipfile

# Liste des codes pays européens (ISO 3166-1 alpha-2)
EUROPEAN_COUNTRIES = {
    'AL', 'AD', 'AT', 'BY', 'BE', 'BA', 'BG', 'HR', 'CY', 'CZ',
    'DK', 'EE', 'FI', 'FR', 'DE', 'GR', 'HU', 'IS', 'IE', 'IT',
    'XK', 'LV', 'LI', 'LT', 'LU', 'MT', 'MD', 'MC', 'ME', 'NL',
    'MK', 'NO', 'PL', 'PT', 'RO', 'RU', 'SM', 'RS', 'SK', 'SI',
    'ES', 'SE', 'CH', 'UA', 'GB', 'VA'
}

# Dossier de destination
OUTPUT_DIR = "downloads_gtfs"

def download_and_extract(url: str, country_code: str, feed_id: str) -> bool:
    """Télécharge un fichier ZIP et l'extrait dans un dossier nommé par pays."""
    try:
        response = requests.get(url, timeout=30, stream=True)
        response.raise_for_status()

        # Créer le dossier du pays
        country_dir = os.path.join(OUTPUT_DIR, country_code)
        os.makedirs(country_dir, exist_ok=True)

        # Télécharger dans un fichier temporaire
        temp_zip = os.path.join(country_dir, f"{feed_id}.zip")
        with open(temp_zip, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        # Extraire le ZIP dans un dossier nommé par feed_id
        extract_dir = os.path.join(country_dir, feed_id)
        os.makedirs(extract_dir, exist_ok=True)

        with zipfile.ZipFile(temp_zip, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)

        # Supprimer le fichier ZIP temporaire
        os.remove(temp_zip)

        print(f"Extrait: {country_code}/{feed_id}")
        return True
    except Exception as e:
        print(f"Erreur pour {country_code}/{feed_id}: {e}")
        return False


def main():
    # Créer la session Spark
    spark = SparkSession.builder \
        .appName("GTFS Europe Downloader") \
        .getOrCreate()

    # Créer le dossier de téléchargement
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Lire le CSV
    df = spark.read.csv(
        "feeds_v2.csv",
        header=True,
        inferSchema=True
    )

    # Filtrer les pays européens avec une URL valide et status actif
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

    print(f"Nombre de feeds européens trouvés: {df_europe.count()}")

    # Collecter les données et télécharger
    feeds = df_europe.collect()

    success_count = 0
    for feed in feeds:
        if download_and_extract(feed['url'], feed['country_code'], feed['id']):
            success_count += 1

    print(f"\nTerminé: {success_count}/{len(feeds)} fichiers téléchargés")

    spark.stop()


if __name__ == "__main__":
    main()
