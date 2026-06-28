# ObRail Europe — Solution applicative ferroviaire (MSPR TPRE532)

Plateforme conteneurisée d'analyse de la mobilité ferroviaire européenne :
comparaison des émissions **CO₂ train vs avion**, exploration des dessertes,
supervision temps réel. Industrialisation du prototype TPRE512 (backend + entrepôt)
en une application complète, testée, supervisée et déployable en une commande.

## Architecture

```
                    ┌────────────┐
   Navigateur ────► │  frontend  │  React/Vite + nginx (:5173)
                    └─────┬──────┘
                          │ /api (proxy nginx)
                    ┌─────▼──────┐
                    │    api     │  FastAPI (:8000)  ──► /metrics
                    └─────┬──────┘
                          │ SQLAlchemy
                    ┌─────▼──────┐
                    │  postgres  │  PostgreSQL 16 (:5433) — schema + seed
                    └────────────┘

  Observabilité :
    api ──logs──► loki (:3100) ─┐
    api ──/metrics──► prometheus (:9090) ─┤──► grafana (:3000)
    postgres ──────────────────┘  dashboards métier + supervision API
```

| Service     | Rôle                              | Port  |
|-------------|-----------------------------------|-------|
| frontend    | Interface React (nginx)           | 5173  |
| api         | API REST FastAPI                  | 8000  |
| postgres    | Base de données (schéma + seed)   | 5433  |
| prometheus  | Métriques HTTP de l'API           | 9090  |
| grafana     | Tableaux de bord                  | 3000  |
| loki        | Agrégation des logs               | 3100  |
| backup      | Sauvegarde auto BDD (pg_dump)     | —     |
| pgadmin     | Administration BDD                | 8080  |

## Prérequis

- Docker + Docker Compose v2

## Lancement (une commande)

```bash
# 1. Copier le modèle d'environnement et renseigner les secrets
cp .env.example .env        # éditer DB_PASSWORD et API_TOKEN

# 2. Démarrer toute la stack
docker compose up -d --build
```

Variables minimales dans `.env` (lues par `docker-compose.yml`) :

```env
DB_USER=postgres
DB_PASSWORD=<mot-de-passe-bdd>
DB_NAME=postgres
API_TOKEN=<jeton-bearer-api>
```

> Le frontend reçoit le même jeton au build via `VITE_API_TOKEN`
> (par défaut égal à `API_TOKEN`).

## Accès

| Interface            | URL                              | Identifiants    |
|----------------------|----------------------------------|-----------------|
| Application web      | http://localhost:5173            | —               |
| API (Swagger)        | http://localhost:8000/docs       | jeton Bearer    |
| Métriques Prometheus | http://localhost:8000/metrics    | public          |
| Grafana              | http://localhost:3000            | admin / admin   |
| Prometheus           | http://localhost:9090            | —               |
| pgAdmin (dev only)   | http://localhost:8080            | cf. `.env`      |

> **pgAdmin** n'est pas dans la stack de production. Pour l'ajouter (dev) :
> `docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d`.

L'API exige un en-tête `Authorization: Bearer <API_TOKEN>` sur `/api/v1/*`.
Les routes `/`, `/docs` et `/metrics` sont publiques.

## Principaux endpoints API

- `GET /api/v1/health` — santé API + connexion BDD
- `GET /api/v1/stations` — gares & aéroports (filtres, pagination)
- `GET /api/v1/routes/train` · `/routes/avion` — liaisons
- `GET /api/v1/compare/journey?dep_city=&arr_city=` — **itinéraire train multi-segments
  (plus court chemin) vs avion direct, CO₂ cumulé**
- `GET /api/v1/ranking/greener-journeys` — top trajets où le train bat l'avion
- `GET /api/v1/stats/network` · `/stats/by-country` · `/stats/night-vs-day`
- `GET /api/v1/metrics` exposé sur `/metrics` (Prometheus)

Documentation interactive complète : `/docs` (OpenAPI/Swagger).

## Tests & qualité du code

```bash
uv sync --all-packages --group dev

# Lint / qualité (Ruff) — périmètre service api/
uv run ruff check api

# Tests unitaires backend (pytest, couverture ≥ 80 %, DB mockée)
uv run pytest

# Tests d'INTÉGRATION (vraie base PostgreSQL seedée)
docker compose -f docker-compose.ci.yml up -d postgres
DB_HOST=localhost DB_PORT=5432 DB_PASSWORD=<pwd> API_TOKEN=test-token \
  uv run pytest -m integration -o addopts="-v"

# Tests E2E frontend (stack démarrée requise)
cd frontend
npm install
npx playwright install --with-deps chromium
BASE_URL=http://localhost:5173 npm run test:e2e
```

Trois niveaux de tests : **unitaires** (logique pure + endpoints mockés),
**intégration** (interopérabilité backend ↔ PostgreSQL réel, marqueur
`@pytest.mark.integration`), **E2E** (parcours navigateur Playwright).

### Analyse SonarQube (self-hosted)

```bash
# 1. Démarrer le serveur Sonar (séparé de l'app)
docker compose -f docker-compose.sonar.yml up -d   # http://localhost:9000

# 2. Générer la couverture puis lancer le scanner
uv run pytest                                        # produit coverage.xml
docker run --rm --network host \
  -e SONAR_HOST_URL=http://localhost:9000 \
  -e SONAR_TOKEN=<token-généré-dans-Sonar> \
  -v "$PWD:/usr/src" sonarsource/sonar-scanner-cli
```

> Prérequis hôte : `sudo sysctl -w vm.max_map_count=262144` (Elasticsearch de Sonar).
> Configuration du scan : [sonar-project.properties](sonar-project.properties).

## CI/CD

Pipeline GitHub Actions ([.github/workflows/pipeline.yml](.github/workflows/pipeline.yml)) :

1. **lint** — Ruff (qualité code, toutes branches, bloquant)
2. **test** — pytest + couverture, export `coverage.xml` (toutes branches)
3. **integration** — PostgreSQL seedé + suite `@pytest.mark.integration`
4. **e2e** — stack Docker + parcours Playwright
5. **build-and-push** — image Docker publiée sur ghcr.io (main)
6. **smoke-test** — stack complète, vérifie API, auth (401) et `/metrics`
7. **sonarqube** — analyse qualité self-hosted (main / déclenchement manuel)

Secrets attendus (repo GitHub) : `DB_PASSWORD`, `API_TOKEN`.

## Supervision

- **Grafana** (:3000) : dashboard *« API ObRail — Supervision »* (disponibilité,
  latence p50/p95/p99, taux d'erreurs 4xx/5xx, débit, logs API) +
  dashboard métier *« RailCarbon »* (KPI, qualité des données, ETL).
- **Prometheus** scrute `/metrics` (instrumentation `prometheus-fastapi-instrumentator`)
  et évalue des **règles d'alerte** ([alert.rules.yml](alert.rules.yml)) : API down,
  taux d'erreurs 5xx > 5%, latence p95 > 1s → détection automatique d'incidents
  (onglet *Alerts* de Prometheus).
- **Loki** collecte les logs applicatifs (`application=railcarbon-api`) et ETL.

## Structure du dépôt

```
api/            API FastAPI (src/api : main, routers, observability, security…)
frontend/       Application React/Vite + tests E2E Playwright
grafana/        Provisioning datasources + dashboards
prometheus.yml  Configuration scrape Prometheus
docker-compose.yml      Stack complète
docker-compose.ci.yml   Stack de smoke-test CI
schema.sql / seed_data.sql   Schéma + données pré-chargées
etl.py          Pipeline ETL (Spark) — alimentation de l'entrepôt
```

## Sécurité & conformité

- Authentification par jeton Bearer ; secrets hors du code (`.env`, variables
  requises au démarrage), modèles versionnés (`.env.example`).
- Validation des entrées (Pydantic / `Query`), gestion d'erreurs HTTP explicite,
  journalisation centralisée (Loki).
- **Conteneurs durcis** : API exécutée en utilisateur non-root, en-têtes de
  sécurité nginx (CSP, X-Frame-Options, nosniff, Referrer-Policy), `.dockerignore`.
- **Rate limiting** (slowapi) : 120 req/min/IP par défaut (`RATE_LIMIT_DEFAULT`),
  réponse `429` au-delà — protection anti-abus / déni de service basique.
- Grafana en lecture anonyme (`Viewer`) pour la démo ; l'édition reste protégée
  par le compte admin (`GRAFANA_PASSWORD`).
- Frontend visant la conformité **RGAA** (liens d'évitement, rôles ARIA,
  alternatives textuelles, focus visible), **vérifiée automatiquement** par
  axe-core dans les tests E2E ([frontend/e2e/a11y.spec.js](frontend/e2e/a11y.spec.js)).
- Aucune donnée personnelle traitée (données ouvertes GTFS / EASA / OPDI).
