import { useState } from 'react';
import HealthBadge      from './components/HealthBadge';
import SearchPanel      from './components/SearchPanel';
import CompareResult    from './components/CompareResult';
import StatsPanel       from './components/StatsPanel';
import NightDayChart    from './components/NightDayChart';
import CountryStatsTable from './components/CountryStatsTable';
import GreenRoutesTable from './components/GreenRoutesTable';
import IncidentsPanel    from './components/IncidentsPanel';
import MapPanel          from './components/MapPanel';

export default function App() {
  const [comparison, setComparison] = useState(null);

  return (
    <>
      {/* Lien d'évitement — RGAA 12.7 */}
      <a href="#main-content" className="skip-link">
        Aller au contenu principal
      </a>

      {/* ── En-tête ─────────────────────────────────────────────────────── */}
      <header className="site-header" role="banner">
        <div className="header-inner">
          <div className="brand">
            <span className="brand-icon" aria-hidden="true">🚆</span>
            <span className="brand-name">ObRail Europe</span>
            <span className="brand-tagline" aria-hidden="true">
              Train vs Avion · Empreinte CO₂
            </span>
          </div>

          <nav aria-label="Navigation principale">
            <ul className="nav-links" role="list">
              <li><a href="#search">Comparer</a></li>
              <li><a href="#map">Carte</a></li>
              <li><a href="#stats">Statistiques</a></li>
              <li><a href="#rankings">Classement</a></li>
              <li><a href="#supervision">Supervision</a></li>
            </ul>
          </nav>

          <HealthBadge />
        </div>
      </header>

      {/* ── Contenu principal ────────────────────────────────────────────── */}
      <main id="main-content">

        {/* Bandeau d'introduction */}
        <section className="hero" aria-labelledby="hero-heading">
          <div className="container hero-inner">
            <p className="hero-eyebrow">Observatoire du ferroviaire européen</p>
            <h1 id="hero-heading" className="hero-title">
              Le train ou l'avion ?<br />
              <span className="hero-accent">Comparez l'empreinte CO₂</span> de vos trajets.
            </h1>
            <p className="hero-lead">
              Données ouvertes harmonisées (GTFS, Back on Track, EASA) pour mesurer
              la contribution du rail à une mobilité bas-carbone en Europe.
            </p>
            <div className="hero-actions">
              <a href="#search" className="btn-primary btn-primary--lg">Comparer un trajet</a>
              <a href="#stats" className="btn-ghost">Voir les statistiques</a>
            </div>
          </div>
        </section>

        {/* Section Comparaison */}
        <section
          id="search"
          aria-labelledby="search-heading"
          className="section section--light"
        >
          <div className="container">
            <h2 id="search-heading">Comparer deux villes</h2>
            <p className="section-desc">
              Entrez une ville de départ et d'arrivée pour comparer l'impact CO₂
              du train et de l'avion par passager.
            </p>

            <SearchPanel onResult={setComparison} />

            {comparison && (
              <CompareResult data={comparison} />
            )}
          </div>
        </section>

        {/* Section Carte */}
        <section
          id="map"
          aria-labelledby="map-heading"
          className="section section--dark"
        >
          <div className="container">
            <h2 id="map-heading">Carte des gares et aéroports</h2>
            <p className="section-desc">
              Cliquez sur un pays pour afficher toutes ses gares (vert) et
              aéroports (rouge) référencés.
            </p>
            <MapPanel />
          </div>
        </section>

        {/* Section Statistiques */}
        <section
          id="stats"
          aria-labelledby="stats-heading"
          className="section section--dark"
        >
          <div className="container">
            <h2 id="stats-heading">Indicateurs du réseau</h2>
            <p className="section-desc">
              Vue d'ensemble des émissions et de la composition du réseau
              ferroviaire européen référencé.
            </p>
            <div className="stats-grid">
              <StatsPanel />
              <NightDayChart />
            </div>

            <h3 className="subsection-title">Dessertes par pays</h3>
            <CountryStatsTable />
          </div>
        </section>

        {/* Section Classement */}
        <section
          id="rankings"
          aria-labelledby="rankings-heading"
          className="section section--light"
        >
          <div className="container">
            <h2 id="rankings-heading">
              Top 10 des trajets les plus écologiques
            </h2>
            <p className="section-desc">
              Routes ferroviaires où choisir le train plutôt que l'avion
              génère le plus d'économies de CO₂ par passager.
            </p>
            <GreenRoutesTable />
          </div>
        </section>

        {/* Section Supervision */}
        <section
          id="supervision"
          aria-labelledby="supervision-heading"
          className="section section--dark"
        >
          <div className="container">
            <h2 id="supervision-heading">Supervision & incidents</h2>
            <p className="section-desc">
              État du service, dernier traitement ETL et contrôles de qualité des
              données, détectés automatiquement par le monitoring.
            </p>
            <IncidentsPanel />
          </div>
        </section>

      </main>

      {/* ── Pied de page ─────────────────────────────────────────────────── */}
      <footer className="site-footer" role="contentinfo">
        <div className="container">
          <p>
            ObRail Europe © 2025 — Sources : GTFS européen, Back on Track, EASA,{' '}
            <abbr title="Open Performance Data Initiative">OPDI</abbr>.{' '}
            Interface conforme{' '}
            <abbr title="Référentiel Général d'Amélioration de l'Accessibilité">
              RGAA
            </abbr>{' '}
            4.1.
          </p>
        </div>
      </footer>
    </>
  );
}
