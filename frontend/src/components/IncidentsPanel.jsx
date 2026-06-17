import { useEffect, useState } from 'react';
import { fetchIncidents } from '../api/client';

/**
 * Vue de supervision : incidents détectés automatiquement (santé service,
 * dernier run ETL, contrôles qualité des données). Rafraîchie toutes les 30 s.
 * Conforme RGAA : role status, aria-live, libellés textuels des sévérités.
 */
const SEVERITY_LABEL = { ok: 'OK', warning: 'Avertissement', error: 'Erreur' };

export default function IncidentsPanel() {
  const [data,    setData]    = useState(null);
  const [loading, setLoading] = useState(true);
  const [error,   setError]   = useState(false);

  useEffect(() => {
    let mounted = true;
    const load = () => {
      fetchIncidents()
        .then((d) => { if (mounted) { setData(d); setError(false); } })
        .catch(() => { if (mounted) setError(true); })
        .finally(() => { if (mounted) setLoading(false); });
    };
    load();
    const id = setInterval(load, 30_000);
    return () => { mounted = false; clearInterval(id); };
  }, []);

  if (loading) {
    return (
      <div className="loading-placeholder" aria-busy="true" aria-label="Chargement des incidents…">
        Analyse de l'état du système…
      </div>
    );
  }

  if (error || !data) {
    return <p className="error-msg" role="alert">Supervision indisponible (API injoignable).</p>;
  }

  const count = data.incident_count ?? 0;
  const items = Array.isArray(data.incidents) ? data.incidents : [];

  return (
    <div className="incidents-panel" role="status" aria-live="polite">
      <div className={`incidents-summary incidents-summary--${count === 0 ? 'ok' : 'alert'}`}>
        <span className="incidents-count" aria-hidden="true">{count}</span>
        <span>
          {count === 0
            ? 'Aucun incident détecté — système nominal'
            : `${count} incident${count > 1 ? 's' : ''} détecté${count > 1 ? 's' : ''}`}
        </span>
      </div>

      <ul className="incidents-list" role="list">
        {items.map((it) => (
          <li key={it.label} className={`incident-item incident-item--${it.severity}`}>
            <span
              className={`incident-dot incident-dot--${it.severity}`}
              aria-hidden="true"
            />
            <span className="incident-label">{it.label}</span>
            <span className="incident-meta">
              {it.value != null && <strong>{it.value.toLocaleString('fr-FR')}</strong>}
              <span className="sr-only">{SEVERITY_LABEL[it.severity]}</span>
              {it.detail && <span className="incident-detail"> · {it.detail}</span>}
            </span>
          </li>
        ))}
      </ul>
    </div>
  );
}
