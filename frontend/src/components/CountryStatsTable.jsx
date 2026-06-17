import { useEffect, useState } from 'react';
import { fetchCountryStats } from '../api/client';

/**
 * Tableau récapitulatif des dessertes par pays.
 * Données issues de /api/v1/stats/by-country.
 * Conforme RGAA : caption sr-only, scope sur th, résumé textuel.
 */

// Codes ISO → libellés FR (mêmes correspondances que les dashboards Grafana)
const PAYS = {
  DE: 'Allemagne', FR: 'France', BE: 'Belgique', NL: 'Pays-Bas', CH: 'Suisse',
  AT: 'Autriche', IT: 'Italie', ES: 'Espagne', PT: 'Portugal', PL: 'Pologne',
  CZ: 'Rép. Tchèque', SK: 'Slovaquie', HU: 'Hongrie', RO: 'Roumanie', SE: 'Suède',
  NO: 'Norvège', DK: 'Danemark', FI: 'Finlande', GB: 'Royaume-Uni', IE: 'Irlande',
  LU: 'Luxembourg', GR: 'Grèce', HR: 'Croatie', SI: 'Slovénie', BG: 'Bulgarie',
};

export default function CountryStatsTable() {
  const [rows,    setRows]    = useState([]);
  const [loading, setLoading] = useState(true);
  const [error,   setError]   = useState(false);

  useEffect(() => {
    fetchCountryStats()
      .then((data) => setRows(Array.isArray(data) ? data.slice(0, 10) : []))
      .catch(() => setError(true))
      .finally(() => setLoading(false));
  }, []);

  if (loading) {
    return (
      <div className="loading-placeholder" aria-busy="true" aria-label="Chargement des statistiques par pays…">
        Chargement des statistiques par pays…
      </div>
    );
  }

  if (error) {
    return <p className="error-msg" role="alert">Statistiques par pays indisponibles.</p>;
  }

  if (!rows.length) {
    return <p>Aucune statistique par pays disponible.</p>;
  }

  const num = (v) => Number(v ?? 0).toLocaleString('fr-FR');

  return (
    <div className="table-wrapper">
      <p className="sr-only">
        Tableau des 10 premiers pays par nombre de dessertes, avec le nombre de gares,
        d'aéroports, de routes ferroviaires et de routes aériennes.
      </p>
      <table className="routes-table" aria-label="Dessertes ferroviaires et aériennes par pays">
        <caption className="sr-only">Top 10 des pays par nombre de stations référencées.</caption>
        <thead>
          <tr>
            <th scope="col">Pays</th>
            <th scope="col">Gares</th>
            <th scope="col">Aéroports</th>
            <th scope="col">Routes train</th>
            <th scope="col">Routes avion</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => (
            <tr key={r.country_code}>
              <td>{PAYS[r.country_code] ?? r.country_code}</td>
              <td>{num(r.nb_train_stations)}</td>
              <td>{num(r.nb_airports)}</td>
              <td>{num(r.nb_train_routes)}</td>
              <td>{num(r.nb_avion_routes)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
