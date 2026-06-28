import { useEffect, useState } from 'react';
import {
  MapContainer,
  TileLayer,
  GeoJSON,
  CircleMarker,
  Popup,
  LayerGroup,
} from 'react-leaflet';
import 'leaflet/dist/leaflet.css';
import { fetchStationsByCountry } from '../api/client';

// GeoJSON des pays d'Europe (frontières cliquables), chargé au runtime.
const EUROPE_GEOJSON =
  'https://raw.githubusercontent.com/leakyMirror/map-of-europe/master/GeoJSON/europe.geojson';

// Lit le code ISO 2 lettres depuis les propriétés d'une feature, quel que soit
// le nom de champ utilisé par le jeu de données.
function isoFromProps(p = {}) {
  const code = p.ISO2 || p.iso_a2 || p.ISO_A2 || p.iso2 || p.id;
  return code ? String(code).toUpperCase() : null;
}

function countryName(p = {}) {
  return p.NAME || p.name || p.admin || p.ADMIN || '';
}

// Style des frontières selon l'état (survol / sélection).
const baseStyle = {
  color: '#2563eb',
  weight: 1,
  fillColor: '#3b82f6',
  fillOpacity: 0.08,
};
const selectedStyle = {
  color: '#1d4ed8',
  weight: 2,
  fillColor: '#1d4ed8',
  fillOpacity: 0.22,
};

export default function MapPanel() {
  const [geo, setGeo] = useState(null);
  const [geoError, setGeoError] = useState(null);

  const [selected, setSelected] = useState(null); // { code, name }
  const [stations, setStations] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  // Chargement du tracé des frontières une seule fois.
  useEffect(() => {
    fetch(EUROPE_GEOJSON)
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then(setGeo)
      .catch((e) => setGeoError(e.message));
  }, []);

  async function selectCountry(code, name) {
    if (!code) return;
    setSelected({ code, name });
    setLoading(true);
    setError(null);
    setStations([]);
    try {
      const data = await fetchStationsByCountry(code);
      setStations(data.filter((s) => s.latitude != null && s.longitude != null));
    } catch (e) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  }

  // Branche les interactions sur chaque pays du GeoJSON.
  function onEachFeature(feature, layer) {
    const code = isoFromProps(feature.properties);
    const name = countryName(feature.properties);
    layer.bindTooltip(name, { sticky: true });
    layer.on({
      click: () => selectCountry(code, name),
      mouseover: (e) => e.target.setStyle({ fillOpacity: 0.25 }),
      mouseout: (e) => {
        const isSel = selected && isoFromProps(feature.properties) === selected.code;
        e.target.setStyle(isSel ? selectedStyle : baseStyle);
      },
    });
  }

  function styleFeature(feature) {
    const isSel = selected && isoFromProps(feature.properties) === selected.code;
    return isSel ? selectedStyle : baseStyle;
  }

  const airports = stations.filter((s) => s.is_airport);
  const gares = stations.filter((s) => !s.is_airport);

  return (
    <div className="map-panel">
      <div className="map-toolbar" aria-live="polite">
        {selected ? (
          <span>
            <strong>{selected.name || selected.code}</strong> —{' '}
            {loading
              ? 'chargement…'
              : `${gares.length} gares · ${airports.length} aéroports`}
          </span>
        ) : (
          <span>Cliquez sur un pays pour afficher ses gares et aéroports.</span>
        )}
        {error && <span className="map-error"> Erreur : {error}</span>}
        {geoError && (
          <span className="map-error"> Carte indisponible : {geoError}</span>
        )}
      </div>

      <MapContainer
        center={[50, 10]}
        zoom={4}
        style={{ height: 540, width: '100%', borderRadius: 12 }}
        scrollWheelZoom
      >
        <TileLayer
          attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
          url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
        />

        {geo && (
          <GeoJSON
            key={selected?.code || 'none'}
            data={geo}
            style={styleFeature}
            onEachFeature={onEachFeature}
          />
        )}

        <LayerGroup>
          {gares.map((s) => (
            <CircleMarker
              key={`g-${s.station_id}`}
              center={[s.latitude, s.longitude]}
              radius={4}
              pathOptions={{ color: '#16a34a', fillColor: '#22c55e', fillOpacity: 0.9 }}
            >
              <Popup>
                <strong>🚉 {s.station_name}</strong>
                <br />
                {s.city || '—'} ({s.country_code})
              </Popup>
            </CircleMarker>
          ))}
          {airports.map((s) => (
            <CircleMarker
              key={`a-${s.station_id}`}
              center={[s.latitude, s.longitude]}
              radius={5}
              pathOptions={{ color: '#b91c1c', fillColor: '#ef4444', fillOpacity: 0.9 }}
            >
              <Popup>
                <strong>✈️ {s.station_name}</strong>
                <br />
                {s.city || '—'} ({s.country_code})
              </Popup>
            </CircleMarker>
          ))}
        </LayerGroup>
      </MapContainer>

      <div className="map-legend">
        <span><span className="dot dot--gare" /> Gares</span>
        <span><span className="dot dot--airport" /> Aéroports</span>
      </div>
    </div>
  );
}
