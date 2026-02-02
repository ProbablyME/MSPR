from flask import Flask, jsonify, render_template_string
import psycopg2
import psycopg2.extras

app = Flask(__name__)

DB_CONFIG = {
    "host": "localhost",
    "port": "5432",
    "database": "postgres",
    "user": "postgres",
    "password": "lpironti"
}

HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Carte des trains GTFS</title>
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
    <style>
        body { margin: 0; padding: 0; }
        #map { width: 100%; height: 100vh; }
        .info-box {
            position: absolute;
            top: 10px;
            right: 10px;
            background: white;
            padding: 15px;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.2);
            z-index: 1000;
            max-width: 300px;
        }
        .legend {
            position: absolute;
            bottom: 30px;
            left: 10px;
            background: white;
            padding: 10px;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.2);
            z-index: 1000;
        }
    </style>
</head>
<body>
    <div id="map"></div>
    <div class="info-box">
        <h3>Statistiques</h3>
        <div id="stats">Chargement...</div>
    </div>
    <div class="legend">
        <b>Lignes de train</b><br>
        <span style="color: #e74c3c;">●</span> Stations<br>
        <span style="color: #3498db;">—</span> Connexions
    </div>

    <script>
        // Centrer sur la France
        var map = L.map('map').setView([46.603354, 1.888334], 6);

        L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
            attribution: '© OpenStreetMap contributors'
        }).addTo(map);

        // Charger les stations
        fetch('/api/stations')
            .then(r => r.json())
            .then(data => {
                data.forEach(s => {
                    if (s.latitude && s.longitude) {
                        L.circleMarker([s.latitude, s.longitude], {
                            radius: 4,
                            fillColor: '#e74c3c',
                            color: '#c0392b',
                            weight: 1,
                            fillOpacity: 0.8
                        }).bindPopup('<b>' + s.nom_station + '</b><br>ID: ' + s.id_station)
                          .addTo(map);
                    }
                });
            });

        // Charger les lignes (connexions entre stations)
        fetch('/api/lignes')
            .then(r => r.json())
            .then(data => {
                data.forEach(ligne => {
                    if (ligne.coords && ligne.coords.length > 1) {
                        L.polyline(ligne.coords, {
                            color: '#3498db',
                            weight: 2,
                            opacity: 0.7
                        }).bindPopup('<b>' + ligne.nom_ligne + '</b>')
                          .addTo(map);
                    }
                });
            });

        // Charger les stats
        fetch('/api/stats')
            .then(r => r.json())
            .then(data => {
                document.getElementById('stats').innerHTML =
                    '<p>Stations: <b>' + data.stations + '</b></p>' +
                    '<p>Lignes: <b>' + data.lignes + '</b></p>' +
                    '<p>Trajets: <b>' + data.trajets + '</b></p>' +
                    '<p>Arrets: <b>' + data.arrets + '</b></p>';
            });
    </script>
</body>
</html>
"""

def get_db():
    return psycopg2.connect(**DB_CONFIG)

@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)

@app.route('/api/stats')
def stats():
    conn = get_db()
    cur = conn.cursor()

    stats = {}
    for table in ['station', 'ligne', 'trajet', 'arret_horaire']:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        key = table if table != 'arret_horaire' else 'arrets'
        key = key + 's' if key in ['station', 'ligne', 'trajet'] else key
        stats[key] = cur.fetchone()[0]

    cur.close()
    conn.close()
    return jsonify(stats)

@app.route('/api/stations')
def stations():
    conn = get_db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT DISTINCT s.id_station, s.nom_station, s.latitude, s.longitude
        FROM station s
        JOIN arret_horaire ah ON s.id_station = ah.id_station
        WHERE s.latitude IS NOT NULL AND s.longitude IS NOT NULL
        LIMIT 5000
    """)

    result = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify(result)

@app.route('/api/lignes')
def lignes():
    conn = get_db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Recuperer les trajets avec leurs arrets ordonnes
    cur.execute("""
        SELECT
            l.id_ligne,
            l.nom_ligne,
            t.id_trajet,
            s.latitude,
            s.longitude,
            ah.sequence_arret
        FROM ligne l
        JOIN trajet t ON l.id_ligne = t.id_ligne
        JOIN arret_horaire ah ON t.id_trajet = ah.id_trajet
        JOIN station s ON ah.id_station = s.id_station
        WHERE s.latitude IS NOT NULL AND s.longitude IS NOT NULL
        ORDER BY l.id_ligne, t.id_trajet, ah.sequence_arret
        LIMIT 50000
    """)

    rows = cur.fetchall()
    cur.close()
    conn.close()

    # Grouper par ligne et trajet
    lignes_map = {}
    for row in rows:
        lid = row['id_ligne']
        if lid not in lignes_map:
            lignes_map[lid] = {
                'nom_ligne': row['nom_ligne'],
                'coords': []
            }
        lignes_map[lid]['coords'].append([row['latitude'], row['longitude']])

    # Dedupliquer les coords consecutives identiques
    result = []
    for lid, data in lignes_map.items():
        coords = data['coords']
        if len(coords) > 1:
            # Garder seulement les coords uniques consecutives
            unique_coords = [coords[0]]
            for c in coords[1:]:
                if c != unique_coords[-1]:
                    unique_coords.append(c)
            if len(unique_coords) > 1:
                result.append({
                    'id_ligne': lid,
                    'nom_ligne': data['nom_ligne'],
                    'coords': unique_coords
                })

    return jsonify(result)

if __name__ == '__main__':
    print("Serveur de visualisation demarre sur http://localhost:5000")
    app.run(debug=True, port=5000)
