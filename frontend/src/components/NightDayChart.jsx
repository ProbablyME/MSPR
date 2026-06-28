import { useEffect, useState } from 'react';
import { Doughnut } from 'react-chartjs-2';
import { Chart as ChartJS, ArcElement, Tooltip, Legend } from 'chart.js';
import { fetchNightVsDay } from '../api/client';

ChartJS.register(ArcElement, Tooltip, Legend);

/**
 * Graphique en anneau : répartition trains de jour / nuit.
 * Conforme RGAA : alternative textuelle (sr-only), graphique marqué aria-hidden,
 * données numériques accessibles via une liste dl visible.
 */
export default function NightDayChart() {
  const [data,    setData]    = useState(null);
  const [loading, setLoading] = useState(true);
  const [error,   setError]   = useState(false);

  useEffect(() => {
    fetchNightVsDay()
      .then(setData)
      .catch(() => setError(true))
      .finally(() => setLoading(false));
  }, []);

  if (loading) {
    return (
      <div
        className="chart-panel loading-placeholder"
        aria-busy="true"
        aria-label="Chargement du graphique jour / nuit…"
      >
        Chargement…
      </div>
    );
  }

  if (error || !data) {
    return (
      <div className="chart-panel">
        <p className="error-msg" role="alert">Données de répartition indisponibles.</p>
      </div>
    );
  }

  // L'API /stats/night-vs-day renvoie un tableau :
  // [{ type:'jour', nb_routes, avg_co2_kg_passenger, ... }, { type:'nuit', ... }]
  const rows  = Array.isArray(data) ? data : [];
  const day   = rows.find((r) => r.type === 'jour')?.nb_routes ?? 0;
  const night = rows.find((r) => r.type === 'nuit')?.nb_routes ?? 0;
  const dayCo2   = rows.find((r) => r.type === 'jour')?.avg_co2_kg_passenger;
  const nightCo2 = rows.find((r) => r.type === 'nuit')?.avg_co2_kg_passenger;
  const total = day + night;

  const pct = (n) => (total > 0 ? ((n / total) * 100).toFixed(1) : '0.0');

  const chartData = {
    labels: ['Trains de jour', 'Trains de nuit'],
    datasets: [{
      data: [day, night],
      backgroundColor: ['#166534', '#1e40af'],
      borderColor:     ['#14532d', '#1e3a8a'],
      borderWidth: 2,
      hoverOffset: 6,
    }],
  };

  const options = {
    responsive: true,
    cutout: '65%',
    plugins: {
      legend: {
        position: 'bottom',
        labels: {
          color: '#0f172a',
          font: { size: 14 },
          padding: 20,
          usePointStyle: true,
        },
      },
      tooltip: {
        callbacks: {
          label: (ctx) => {
            const p = total > 0 ? ((ctx.parsed / total) * 100).toFixed(1) : 0;
            return ` ${ctx.label} : ${ctx.parsed.toLocaleString('fr-FR')} (${p} %)`;
          },
        },
      },
    },
  };

  return (
    <div className="chart-panel">
      <h3 className="panel-title">Répartition jour / nuit</h3>

      {/* Alternative textuelle pour les lecteurs d'écran — RGAA 4.1 */}
      <p className="sr-only">
        Graphique en anneau montrant la répartition entre trains de jour et de nuit :
        {day.toLocaleString('fr-FR')} trains de jour ({pct(day)} %),
        {night.toLocaleString('fr-FR')} trains de nuit ({pct(night)} %).
        Total : {total.toLocaleString('fr-FR')} trains.
      </p>

      {/* Graphique marqué aria-hidden car l'alternative textuelle est fournie */}
      <div aria-hidden="true" className="chart-wrapper">
        <Doughnut data={chartData} options={options} />
      </div>

      {/* Récapitulatif chiffré visible accessible */}
      <dl className="chart-legend">
        <div>
          <dt>Jour</dt>
          <dd>
            {day.toLocaleString('fr-FR')} <small>({pct(day)} %)</small>
            {dayCo2 != null && <small className="legend-co2">{Number(dayCo2).toFixed(2)} kg CO₂/pax</small>}
          </dd>
        </div>
        <div>
          <dt>Nuit</dt>
          <dd>
            {night.toLocaleString('fr-FR')} <small>({pct(night)} %)</small>
            {nightCo2 != null && <small className="legend-co2">{Number(nightCo2).toFixed(2)} kg CO₂/pax</small>}
          </dd>
        </div>
      </dl>
    </div>
  );
}
