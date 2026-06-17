/**
 * Affiche le résultat de comparaison CO₂ train vs avion.
 *
 * Reçoit `journey` (objet /compare/journey) :
 *   { found, dep_city, arr_city, nb_segments, total_distance_km, train_co2_kg,
 *     path_cities, plane_co2_kg, plane_distance_km, greener_mode,
 *     savings_kg, savings_percent }
 *
 * Le train est reconstruit par plus court chemin sur les tronçons gare→gare,
 * le CO₂ étant la somme des émissions de chaque segment.
 *
 * Conforme RGAA : section avec heading, aria-live, cartes avec aria-label complets.
 */
export default function CompareResult({ data }) {
  const { journey, from, to } = data;
  if (!journey) return null;

  const trainCo2 = journey.train_co2_kg ?? null;
  const planeCo2 = journey.plane_co2_kg ?? null;
  const greener  = journey.greener_mode;
  const trainWins = greener === 'train';
  const savings    = journey.savings_kg;
  const savingsPct = journey.savings_percent;
  const nbSeg    = journey.nb_segments ?? 0;
  const trainKm  = journey.total_distance_km;
  const planeKm  = journey.plane_distance_km;
  const pathCities = Array.isArray(journey.path_cities) ? journey.path_cities : [];

  const fmt = (v) => (v != null ? Number(v).toFixed(1) : '—');

  return (
    <section
      className="compare-result"
      aria-labelledby="compare-result-heading"
      aria-live="polite"
      aria-atomic="true"
    >
      <h3 id="compare-result-heading">Résultat : {from} → {to}</h3>

      <div className="compare-cards">
        {/* Carte Train */}
        <article
          className={`transport-card transport-card--train${trainWins ? ' transport-card--winner' : ''}`}
          aria-label={`Train : ${fmt(trainCo2)} kg CO₂ par passager${trainWins ? ' — mode le plus écologique' : ''}`}
        >
          {trainWins && (
            <span className="winner-badge" aria-hidden="true">✓ Plus écologique</span>
          )}
          <span className="transport-icon" aria-hidden="true">🚆</span>
          <h4>Train</h4>
          <p className="co2-value">
            <strong>{fmt(trainCo2)}</strong> <span>kg CO₂ / passager</span>
          </p>
          {journey.found && (
            <p className="transport-detail">
              {trainKm != null && <>{Math.round(trainKm)} km · </>}
              {nbSeg} tronçon{nbSeg > 1 ? 's' : ''}
              {nbSeg > 1 && ' (avec correspondances)'}
            </p>
          )}
          {!journey.found && (
            <p className="transport-detail">Aucun itinéraire ferroviaire trouvé</p>
          )}
        </article>

        <div className="compare-vs" aria-hidden="true">VS</div>

        {/* Carte Avion */}
        <article
          className={`transport-card transport-card--plane${greener === 'avion' ? ' transport-card--winner' : ''}`}
          aria-label={`Avion : ${fmt(planeCo2)} kg CO₂ par passager${greener === 'avion' ? ' — mode le plus écologique' : ''}`}
        >
          {greener === 'avion' && (
            <span className="winner-badge" aria-hidden="true">✓ Plus écologique</span>
          )}
          <span className="transport-icon" aria-hidden="true">✈️</span>
          <h4>Avion</h4>
          <p className="co2-value">
            <strong>{fmt(planeCo2)}</strong> <span>kg CO₂ / passager</span>
          </p>
          {planeKm != null && (
            <p className="transport-detail">{Math.round(planeKm)} km · vol direct</p>
          )}
          {planeCo2 == null && (
            <p className="transport-detail">Aucune liaison aérienne directe</p>
          )}
        </article>
      </div>

      {/* Détail de l'itinéraire ferroviaire reconstruit */}
      {journey.found && pathCities.length > 1 && (
        <p className="compare-path" aria-label={`Itinéraire ferroviaire : ${pathCities.join(', ')}`}>
          <span aria-hidden="true">🛤️ </span>
          {pathCities.join(' → ')}
        </p>
      )}

      {savings != null && savingsPct != null && (
        <p className="compare-summary">
          {trainWins ? '🌱 Prendre le train économise ' : "Prendre l'avion économise "}
          <strong>{Number(savings).toFixed(1)} kg CO₂</strong>
          {' '}({Number(savingsPct).toFixed(0)} %) par passager.
        </p>
      )}

      {savings == null && (trainCo2 != null || planeCo2 != null) && (
        <p className="compare-summary compare-summary--info">
          {trainCo2 != null && planeCo2 == null
            ? "Seul le train relie ce trajet dans nos données — pas de vol direct équivalent."
            : "Seul l'avion relie ce trajet dans nos données — aucun itinéraire ferroviaire trouvé."}
        </p>
      )}
    </section>
  );
}
