#!/usr/bin/env python3
"""
Migration corrective — recalcule ``country_code`` des stations à partir de leurs
coordonnées GPS (géocodage inverse hors-ligne).

Contexte : l'ETL a parfois attribué un mauvais code pays (ex. « Berlin Hbf »
étiqueté SK au lieu de DE) en raison de feeds GTFS hétérogènes. Ce script
corrige ``mart.dim_station`` directement, sans relancer tout le pipeline Spark.

Usage :
    # stack démarrée (Postgres exposé sur 5433)
    uv run --with reverse-geocode --with psycopg2-binary \
        python scripts/fix_country_codes.py

Variables d'environnement (défauts entre parenthèses) :
    DB_HOST (localhost) DB_PORT (5433) DB_NAME (postgres)
    DB_USER (postgres)  DB_PASSWORD (lpironti)

Idempotent : ne met à jour que les lignes dont le pays calculé diffère.
"""
import os

import psycopg2
import reverse_geocode
from psycopg2.extras import execute_values


def main() -> None:
    conn = psycopg2.connect(
        host=os.environ.get("DB_HOST", "localhost"),
        port=os.environ.get("DB_PORT", "5433"),
        dbname=os.environ.get("DB_NAME", "postgres"),
        user=os.environ.get("DB_USER", "postgres"),
        password=os.environ.get("DB_PASSWORD", "lpironti"),
    )
    cur = conn.cursor()
    cur.execute("""
        SELECT station_id, latitude, longitude, country_code
        FROM mart.dim_station
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
    """)
    rows = cur.fetchall()
    print(f"{len(rows)} stations géolocalisées à vérifier…")

    coords = [(float(lat), float(lon)) for _, lat, lon, _ in rows]
    geo = reverse_geocode.search(coords)

    updates = [
        (g["country_code"], sid)
        for (sid, _, _, current), g in zip(rows, geo)
        if g.get("country_code") and g["country_code"] != current
    ]
    print(f"{len(updates)} codes pays à corriger.")

    if updates:
        execute_values(
            cur,
            """
            UPDATE mart.dim_station AS s
            SET country_code = v.cc
            FROM (VALUES %s) AS v(cc, sid)
            WHERE s.station_id = v.sid
            """,
            updates,
        )
        conn.commit()
    print("Terminé.")
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
