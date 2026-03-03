"""
map_flights.py
==============
Génère une carte interactive HTML affichant toutes les routes aériennes
stockées dans mart.dim_route_avion, avec un tracé de ligne entre chaque
paire d'aéroports (coordonnées issues de mart.dim_station).

Sortie : flight_routes_map.html (ouvrir dans un navigateur)
"""

import sys
import logging
from datetime import datetime

import folium
from folium.plugins import HeatMap
import psycopg2

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":     "localhost",
    "port":     "5433",
    "database": "postgres",
    "user":     "postgres",
    "password": "lpironti",
}

OUTPUT_FILE = "flight_routes_map.html"


# ── Requête base ──────────────────────────────────────────────────────────────
QUERY = """
    SELECT
        r.dep_station_id,
        r.arr_station_id,
        r.distance_km,
        dep.latitude   AS dep_lat,
        dep.longitude  AS dep_lon,
        dep.station_name AS dep_name,
        arr.latitude   AS arr_lat,
        arr.longitude  AS arr_lon,
        arr.station_name AS arr_name
    FROM mart.dim_route_avion r
    JOIN mart.dim_station dep ON dep.station_id = r.dep_station_id
    JOIN mart.dim_station arr ON arr.station_id = r.arr_station_id
    WHERE dep.latitude  IS NOT NULL
      AND dep.longitude IS NOT NULL
      AND arr.latitude  IS NOT NULL
      AND arr.longitude IS NOT NULL
    ORDER BY r.distance_km DESC NULLS LAST;
"""


def fetch_routes() -> list[dict]:
    logger.info("Connexion à PostgreSQL...")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
    except psycopg2.OperationalError as e:
        logger.error(f"Connexion impossible : {e}")
        return []

    try:
        with conn.cursor() as cur:
            cur.execute(QUERY)
            cols = [desc[0] for desc in cur.description]
            rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        logger.info(f"{len(rows):,} routes récupérées depuis la BDD.")
        return rows
    finally:
        conn.close()


def build_map(routes: list[dict]) -> folium.Map:
    """
    Construit la carte folium avec :
      - Un PolyLine par route (couleur selon la distance)
      - Un marqueur circulaire par aéroport unique
    """
    # Centre de la carte sur l'Europe
    fmap = folium.Map(
        location=[48.0, 10.0],
        zoom_start=4,
        tiles="CartoDB dark_matter",
    )

    # ── Couleur selon distance ────────────────────────────────────────────────
    def route_color(dist_km: float | None) -> str:
        if dist_km is None:
            return "#888888"
        if dist_km < 500:
            return "#00d4ff"   # bleu clair – court courrier
        if dist_km < 1500:
            return "#00ff99"   # vert – moyen courrier
        if dist_km < 3000:
            return "#ffcc00"   # jaune – long courrier
        return "#ff4444"       # rouge – très long courrier

    # ── Tracé des routes ──────────────────────────────────────────────────────
    airports_seen: dict[str, dict] = {}

    for r in routes:
        dep_coords = (r["dep_lat"], r["dep_lon"])
        arr_coords = (r["arr_lat"], r["arr_lon"])
        dist       = r["distance_km"]
        color      = route_color(dist)
        dist_str   = f"{dist:,.0f} km" if dist else "distance inconnue"

        folium.PolyLine(
            locations=[dep_coords, arr_coords],
            color=color,
            weight=0.6,
            opacity=0.5,
            tooltip=folium.Tooltip(
                f"{r['dep_station_id']} → {r['arr_station_id']} | {dist_str}"
            ),
        ).add_to(fmap)

        # Collecte des aéroports uniques pour les marqueurs
        for icao, coords, name in [
            (r["dep_station_id"], dep_coords, r["dep_name"]),
            (r["arr_station_id"], arr_coords, r["arr_name"]),
        ]:
            if icao not in airports_seen:
                airports_seen[icao] = {"coords": coords, "name": name}

    # ── Marqueurs aéroports ───────────────────────────────────────────────────
    airport_layer = folium.FeatureGroup(name="Aéroports", show=True)
    for icao, info in airports_seen.items():
        folium.CircleMarker(
            location=info["coords"],
            radius=2,
            color="#ffffff",
            fill=True,
            fill_color="#ffffff",
            fill_opacity=0.8,
            tooltip=folium.Tooltip(f"<b>{icao}</b><br>{info['name']}"),
        ).add_to(airport_layer)
    airport_layer.add_to(fmap)

    # ── Légende ───────────────────────────────────────────────────────────────
    legend_html = """
    <div style="
        position: fixed; bottom: 30px; left: 30px; z-index: 1000;
        background: rgba(0,0,0,0.75); color: white;
        padding: 12px 16px; border-radius: 8px;
        font-family: Arial, sans-serif; font-size: 13px; line-height: 1.8;">
        <b>Distance de la route</b><br>
        <span style="color:#00d4ff;">&#9473;</span> &lt; 500 km (court courrier)<br>
        <span style="color:#00ff99;">&#9473;</span> 500 – 1 500 km<br>
        <span style="color:#ffcc00;">&#9473;</span> 1 500 – 3 000 km<br>
        <span style="color:#ff4444;">&#9473;</span> &gt; 3 000 km (long courrier)<br>
        <span style="color:#888888;">&#9473;</span> Distance inconnue
    </div>
    """
    fmap.get_root().html.add_child(folium.Element(legend_html))

    folium.LayerControl().add_to(fmap)
    return fmap


def main() -> None:
    logger.info("=== Génération de la carte des routes aériennes ===")

    routes = fetch_routes()
    if not routes:
        logger.error("Aucune route à afficher. Vérifiez que l'ETL a été exécuté.")
        return

    logger.info("Construction de la carte...")
    fmap = build_map(routes)

    fmap.save(OUTPUT_FILE)
    logger.info(f"Carte sauvegardée : {OUTPUT_FILE}")
    logger.info("Ouvrez le fichier dans votre navigateur pour visualiser.")


if __name__ == "__main__":
    main()
