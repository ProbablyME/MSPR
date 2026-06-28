# Plan de maintenance & rollback — ObRail Europe

Document de référence pour l'exploitation, la mise à jour et la reprise de la
solution en production. Complète le [README](../README.md).

## 1. Versionnement & traçabilité

- **Code** : Git. Toute évolution passe par une branche + Pull Request (CI verte
  obligatoire : tests unitaires, E2E, build). `main` = version de référence.
- **Images Docker** : publiées sur `ghcr.io/<repo>/api` par le pipeline, taguées :
  - `:main` — dernière version stable de `main` ;
  - `:sha-<commit>` — **tag immuable** par commit (utilisé pour le rollback).
- **Base de données** : schéma dans `schema.sql`, données initiales dans
  `seed_data.sql`. Toute modification de schéma = script de migration daté.

## 2. Procédure de mise à jour (release)

1. Merge de la PR sur `main` → le pipeline construit et pousse l'image
   `:sha-<commit>` puis exécute le `smoke-test`.
2. Sur l'environnement cible :
   ```bash
   git pull
   cp .env.example .env          # si nouvelles variables
   docker compose pull           # récupère les images publiées
   docker compose up -d --build  # applique la nouvelle version
   ```
3. **Vérifications post-déploiement** (voir §5).

## 3. Rollback

Objectif : revenir à la dernière version saine **en < 5 minutes**.

### 3.1 Rollback applicatif (API / frontend)

Le tag `:sha-<commit>` étant immuable, on redéploie la version précédente :

```bash
# Repérer le commit précédent stable
git log --oneline -n 5

# Option A — via l'image publiée (recommandé)
export API_IMAGE=ghcr.io/<repo>/api:sha-<commit_precedent>
docker compose -f docker-compose.ci.yml up -d api

# Option B — via le code
git checkout <commit_precedent>
docker compose up -d --build api frontend
```

### 3.2 Rollback base de données

- **Données** (volume `railcarbon_pgdata`) : restaurer le dernier dump (§4).
  ```bash
  # Les dumps sont compressés (.sql.gz) — décompresser à la volée :
  gzip -dc backups/dump_<date>.sql.gz | docker exec -i railcarbon_db psql -U postgres -d postgres
  ```
- **Réinitialisation complète** (dernier recours — perte des données runtime) :
  ```bash
  docker compose down
  docker volume rm mspr_railcarbon_pgdata
  docker compose up -d            # ré-initialise schema.sql + seed_data.sql
  ```

### 3.3 Critères de déclenchement d'un rollback

- `smoke-test` rouge après déploiement ;
- `/api/v1/health` ≠ 200 ou taux d'erreurs 5xx > 5 % (dashboard Grafana) ;
- incident critique remonté par `/api/v1/monitoring/incidents`.

## 4. Sauvegardes

**Automatisées** par le service `backup` (sidecar `postgres:16-alpine`) lancé
avec la stack (`docker compose up -d`). Il exécute [scripts/backup_db.sh](../scripts/backup_db.sh) :

- `pg_dump` compressé (`.sql.gz`) à chaque démarrage puis **toutes les 24 h**
  (`BACKUP_INTERVAL`, en secondes) ;
- écrit dans `./backups/dump_<horodatage>.sql.gz` (monté côté hôte) ;
- **rotation** : suppression des dumps de plus de `BACKUP_KEEP_DAYS` jours (7 par défaut).

```bash
# Backup manuel immédiat (one-shot)
docker compose run --rm -e BACKUP_INTERVAL=0 backup

# Lister les sauvegardes
ls -lh backups/
```

Variables (`.env`) : `BACKUP_INTERVAL` (défaut 86400), `BACKUP_KEEP_DAYS` (défaut 7).

- Persistance des volumes : `railcarbon_pgdata`, `railcarbon_grafana_data`,
  `railcarbon_loki_data`, `railcarbon_prometheus_data`.
- ⚠️ Persistance ≠ sauvegarde : le service `backup` protège d'une corruption /
  suppression de volume, ce que le volume seul ne couvre pas.

## 5. Vérifications post-déploiement (checklist)

| Contrôle | Commande / URL | Attendu |
|----------|----------------|---------|
| Santé API | `GET /api/v1/health` (avec token) | `status: ok` |
| Auth | `GET /api/v1/stations` sans token | `401/403` |
| Métriques | `GET /metrics` | contient `http_request_duration_seconds` |
| Frontend | http://localhost:5173 | page chargée, badge « API en ligne » |
| Supervision | Grafana « API ObRail — Supervision » | pas de 5xx, latence p95 normale |
| Incidents | `GET /api/v1/monitoring/incidents` | `incident_count` maîtrisé |

## 6. Supervision & feedback loop

- **Grafana** (`:3000`) — dashboards API (latence, erreurs, débit, logs) et
  métier (qualité des données, ETL).
- **Prometheus** (`:9090`) — métriques HTTP de l'API.
- **Loki** — logs applicatifs (`application=railcarbon-api`) et ETL.
- **API** — `/monitoring/incidents` agrège les anomalies (santé, ETL, qualité)
  consommées par la vue Supervision du frontend.

## 7. Migrations correctives connues

| Script | Objet | Quand l'exécuter |
|--------|-------|------------------|
| [`scripts/fix_country_codes.py`](../scripts/fix_country_codes.py) | Recalcule `country_code` des stations depuis les coordonnées (corrige les codes pays erronés issus de l'ETL) | Après un (re)chargement complet du seed/ETL |
| [`seed_avion_co2_fix.sql`](../seed_avion_co2_fix.sql) | Recalcule les émissions **avion** avec des facteurs ADEME (kg CO2e/pax.km). Le seed historique utilisait la « CO2 Metric Value » EASA (métrique de certification) qui sous-estimait les émissions → l'avion ressortait à tort plus écologique que le train. Appliqué **automatiquement** comme script d'init (`04_fix_avion_co2.sql`). | Auto au 1er démarrage ; rejouable (idempotent) sur une base existante |

```bash
# Migration codes pays
uv run --with reverse-geocode --with psycopg2-binary \
    python scripts/fix_country_codes.py

# Migration émissions avion (sur une base déjà chargée)
docker exec -i railcarbon_db psql -U postgres -d postgres < seed_avion_co2_fix.sql
```

## 8. Incidents fréquents & remédiation

| Symptôme | Cause probable | Action |
|----------|----------------|--------|
| Frontend : requêtes 401 | `VITE_API_TOKEN` ≠ `API_TOKEN` au build | rebuild frontend avec le bon jeton |
| API ne démarre pas | Postgres pas prêt / `DB_PASSWORD` manquant | vérifier `.env`, healthcheck `postgres` |
| Grafana « No data » API | Prometheus ne scrute pas l'API | vérifier `prometheus.yml` et la cible `up` |
| Distances/itinéraires aberrants | collisions de `stop_id` entre feeds GTFS | déjà filtré côté API ; correction de fond = namespacing `stop_id` à l'ETL |
| `country_code` faux | géocodage ETL | exécuter `scripts/fix_country_codes.py` (§7) |
