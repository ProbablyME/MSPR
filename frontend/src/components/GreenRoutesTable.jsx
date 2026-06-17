import { useEffect, useState } from 'react';
import { fetchGreenRoutes } from '../api/client';

/**
 * Tableau des 10 trajets où le train génère le moins de CO₂ vs l'avion.
 * Conforme RGAA : caption, scope sur th, résumé sr-only, tri accessible.
 */
export default function GreenRoutesTable() {
  const [routes,  setRoutes]  = useState([]);
  const [loading, setLoading] = useState(true);
  const [error,   setError]   = useState(false);

  useEffect(() => {
    fetchGreenRoutes(10)
      // /ranking/greener-routes renvoie un tableau brut (pas un objet paginé)
      .then((data) => setRoutes(Array.isArray(data) ? data : (data.data ?? [])))
      .catch(() => setError(true))
      .finally(() => setLoading(false));
  }, []);

  if (loading) {
    return (
      <div
        className="loading-placeholder"
        aria-busy="true"
        aria-label="Chargement du classement des routes écologiques…"
      >
        Chargement du classement…
      </div>
    );
  }

  if (error) {
    return (
      <p className="error-msg" role="alert">
        Impossible de charger le classement. Vérifiez la connexion à l'API.
      </p>
    );
  }

  if (!routes.length) {
    return <p>Aucune donnée de classement disponible pour le moment.</p>;
  }

  const fmt = (v) => (v != null ? Number(v).toFixed(1) : '—');

  return (
    <div className="table-wrapper">
      {/* Résumé sr-only pour lecteurs d'écran — RGAA 5.6 */}
      <p className="sr-only">
        Tableau des 10 trajets ferroviaires européens les plus écologiques,
        classés par économie de CO₂ décroissante par rapport à l'avion équivalent.
        Colonnes : rang, ville de départ, ville d'arrivée, CO₂ train en kg,
        CO₂ avion en kg, économie en pourcentage.
      </p>

      <table
        className="routes-table"
        aria-label="Top 10 des trajets ferroviaires les plus écologiques"
      >
        <caption className="sr-only">
          Classement des 10 routes où le train émet le moins de CO₂ comparé à l'avion.
          Données issues du réseau ferroviaire européen (GTFS + Back on Track).
        </caption>
        <thead>
          <tr>
            <th scope="col" abbr="Rang">#</th>
            <th scope="col">Départ</th>
            <th scope="col">Arrivée</th>
            <th scope="col">CO₂ train (kg)</th>
            <th scope="col">CO₂ avion (kg)</th>
            <th scope="col">Économie (%)</th>
          </tr>
        </thead>
        <tbody>
          {routes.map((route, i) => (
            <tr key={`${route.dep_city}-${route.arr_city}`}>
              <td aria-label={`Rang ${i + 1}`}>
                <span className="rank-badge" aria-hidden="true">{i + 1}</span>
              </td>
              <td>{route.dep_city ?? '—'}</td>
              <td>{route.arr_city ?? '—'}</td>
              <td>{fmt(route.train_co2_kg)}</td>
              <td>{fmt(route.plane_co2_kg)}</td>
              <td>
                <span
                  className="savings-pct"
                  aria-label={`${route.savings_percent != null ? route.savings_percent.toFixed(0) : '—'} pour cent d'économie`}
                >
                  {route.savings_percent != null
                    ? `${route.savings_percent.toFixed(0)} %`
                    : '—'}
                </span>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
