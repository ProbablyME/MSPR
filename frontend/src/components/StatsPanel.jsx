import { useState, useEffect } from 'react';
import { fetchNetworkStats } from '../api/client';

/** Carte d'un indicateur chiffré */
function StatCard({ label, value, unit, description }) {
  return (
    <article
      className="stat-card"
      aria-label={`${label} : ${value}${unit ? ' ' + unit : ''}`}
    >
      <p className="stat-label" aria-hidden="true">{label}</p>
      <p className="stat-value">
        <strong>{value}</strong>
        {unit && <span className="stat-unit"> {unit}</span>}
      </p>
      {description && <p className="stat-desc">{description}</p>}
    </article>
  );
}

/**
 * Panneau des indicateurs clés du réseau ferroviaire européen.
 * Données issues de /api/v1/stats/network.
 */
export default function StatsPanel() {
  const [stats,   setStats]   = useState(null);
  const [loading, setLoading] = useState(true);
  const [error,   setError]   = useState(false);

  useEffect(() => {
    fetchNetworkStats()
      .then(setStats)
      .catch(() => setError(true))
      .finally(() => setLoading(false));
  }, []);

  if (loading) {
    return (
      <div
        className="loading-placeholder"
        aria-busy="true"
        aria-label="Chargement des statistiques réseau…"
      >
        Chargement des statistiques…
      </div>
    );
  }

  if (error || !stats) {
    return (
      <p className="error-msg" role="alert">
        Impossible de charger les statistiques. Vérifiez la connexion à l'API.
      </p>
    );
  }

  const fmt = (v, d = 1) => (v != null ? Number(v).toFixed(d) : '—');
  const intFmt = (v) => (v != null ? Number(v).toLocaleString('fr-FR') : '—');

  return (
    <div className="stats-panel">
      <h3 className="panel-title">Réseau ferroviaire européen</h3>
      <div className="stat-cards">
        <StatCard
          label="Routes ferroviaires"
          value={intFmt(stats.total_train_routes)}
          description="Liaisons train référencées"
        />
        <StatCard
          label="Routes aériennes"
          value={intFmt(stats.total_plane_routes)}
          description="Liaisons avion référencées"
        />
        <StatCard
          label="CO₂ moyen — train"
          value={fmt(stats.avg_train_co2_kg)}
          unit="kg / passager"
          description="Émission moyenne par trajet ferroviaire"
        />
        <StatCard
          label="CO₂ moyen — avion"
          value={fmt(stats.avg_plane_co2_kg)}
          unit="kg / passager"
          description="Émission moyenne par vol commercial"
        />
        <StatCard
          label="Trajets comparables"
          value={intFmt(stats.total_comparable_routes)}
          description="Villes desservies par train ET avion"
        />
        <StatCard
          label="Économie moyenne (train)"
          value={fmt(stats.avg_savings_percent, 0)}
          unit="%"
          description="CO₂ économisé sur les trajets comparables"
        />
      </div>
    </div>
  );
}
