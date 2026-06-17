/**
 * Client API ObRail Europe
 * Toutes les requêtes passent par /api (proxifié par nginx en prod,
 * par le dev-server Vite en local).
 */

const BASE = '/api/v1';

// Le token vient exclusivement d'une variable d'environnement Vite (build-time).
// Aucun secret en dur : voir frontend/.env.example
const TOKEN = import.meta.env.VITE_API_TOKEN;
if (!TOKEN) {
  console.warn('VITE_API_TOKEN non défini — les requêtes API échoueront (401).');
}

const HEADERS = {
  Authorization: `Bearer ${TOKEN}`,
  'Content-Type': 'application/json',
};

async function get(path) {
  const response = await fetch(`${BASE}${path}`, { headers: HEADERS });
  if (!response.ok) {
    throw new Error(`HTTP ${response.status} — ${path}`);
  }
  return response.json();
}

// ── Santé ──────────────────────────────────────────────────────────────────
export const fetchHealth = () => get('/health');

// ── Statistiques ───────────────────────────────────────────────────────────
export const fetchNetworkStats = () => get('/stats/network');
export const fetchNightVsDay   = () => get('/stats/night-vs-day');
export const fetchCountryStats = () => get('/stats/by-country');

// ── Classements ────────────────────────────────────────────────────────────
export const fetchGreenRoutes = (limit = 10) =>
  get(`/ranking/greener-journeys?limit=${limit}`);

// ── Recherche ──────────────────────────────────────────────────────────────
export function searchCities(q = '') {
  return get(`/search/cities?q=${encodeURIComponent(q)}&limit=10`);
}

// ── Comparaison ────────────────────────────────────────────────────────────
export function compareCities(from, to) {
  return get(
    `/compare/cities?dep_city=${encodeURIComponent(from)}&arr_city=${encodeURIComponent(to)}`,
  );
}

export function compareWinner(from, to) {
  return get(
    `/compare/winner?dep_city=${encodeURIComponent(from)}&arr_city=${encodeURIComponent(to)}`,
  );
}

// Itinéraire train multi-segments (plus court chemin, CO₂ cumulé) vs avion direct
export function compareJourney(from, to) {
  return get(
    `/compare/journey?dep_city=${encodeURIComponent(from)}&arr_city=${encodeURIComponent(to)}`,
  );
}
