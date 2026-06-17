import { useState, useEffect } from 'react';
import { fetchHealth } from '../api/client';

/**
 * Indicateur temps réel de l'état de l'API.
 * Vérifie /api/v1/health toutes les 30 secondes.
 * Conforme RGAA : rôle status, aria-live, aria-label textuel.
 */
export default function HealthBadge() {
  const [status, setStatus] = useState('loading'); // 'online' | 'offline' | 'loading'

  useEffect(() => {
    let mounted = true;

    const check = () => {
      fetchHealth()
        .then((data) => {
          if (mounted) setStatus(data.status === 'ok' ? 'online' : 'offline');
        })
        .catch(() => {
          if (mounted) setStatus('offline');
        });
    };

    check();
    const interval = setInterval(check, 30_000);
    return () => {
      mounted = false;
      clearInterval(interval);
    };
  }, []);

  const labels = {
    loading: 'Vérification…',
    online:  'API en ligne',
    offline: 'API hors ligne',
  };

  return (
    <div
      className={`health-badge health-badge--${status}`}
      role="status"
      aria-live="polite"
      aria-label={`État du service : ${labels[status]}`}
    >
      <span className={`health-dot health-dot--${status}`} aria-hidden="true" />
      <span className="health-label">{labels[status]}</span>
    </div>
  );
}
