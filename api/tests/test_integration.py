"""
Tests d'INTÉGRATION — exécutés contre une vraie base PostgreSQL seedée
(schema.sql + seed_data.sql), via la stack `docker-compose.ci.yml`.

Contrairement aux tests d'endpoints (`conftest.py`, DB mockée), ceux-ci
vérifient l'interopérabilité réelle backend ↔ base de données : connexion
SQLAlchemy, requêtes SQL, schéma `mart.*` et cohérence des données chargées.

Lancement local :
    docker compose -f docker-compose.ci.yml up -d postgres
    DB_HOST=localhost DB_PORT=5432 DB_PASSWORD=... API_TOKEN=test-token \
        uv run pytest -m integration

Sans base joignable, tout le module est ignoré (skip) — la CI « unitaire »
reste verte hors environnement Docker.
"""
import os

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

# conftest a déjà fixé API_TOKEN=test-token avant l'import de l'app.
from api.database import engine
from api.main import app

TOKEN = os.environ.get("API_TOKEN", "test-token")
HEADERS = {"Authorization": f"Bearer {TOKEN}"}

pytestmark = pytest.mark.integration


def _db_reachable() -> bool:
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False


# Si la base n'est pas joignable (dev sans Docker), on ignore tout le module.
if not _db_reachable():
    pytest.skip(
        "PostgreSQL indisponible — tests d'intégration ignorés "
        "(démarrer docker-compose.ci.yml).",
        allow_module_level=True,
    )


@pytest.fixture(scope="module")
def client():
    # Pas d'override de get_db : on tape la vraie base.
    with TestClient(app) as c:
        yield c


# ── Connexion / santé ─────────────────────────────────────────────────────────

def test_health_real_db(client):
    r = client.get("/api/v1/health", headers=HEADERS)
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert body["database"] == "connected"


def test_schema_mart_tables_exist():
    """Le schéma `mart` et ses tables principales sont bien chargés."""
    expected = {"dim_station", "dim_route_train", "dim_route_avion", "fact_emission"}
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'mart'"
        )).fetchall()
    tables = {r[0] for r in rows}
    assert expected.issubset(tables), f"Tables manquantes : {expected - tables}"


# ── Endpoints sur données réelles ─────────────────────────────────────────────

def test_stations_returns_seeded_rows(client):
    r = client.get("/api/v1/stations?per_page=5", headers=HEADERS)
    assert r.status_code == 200
    body = r.json()
    assert body["total"] > 0
    assert len(body["data"]) > 0
    first = body["data"][0]
    assert {"station_id", "station_name", "country_code", "is_airport"} <= first.keys()


def test_stats_network_consistent(client):
    r = client.get("/api/v1/stats/network", headers=HEADERS)
    assert r.status_code == 200
    body = r.json()
    # Cohérence : un sous-ensemble comparable ≤ chaque total de routes.
    assert body["total_comparable_routes"] <= body["total_train_routes"]
    assert body["total_comparable_routes"] <= body["total_plane_routes"]
    assert body["avg_train_co2_kg"] >= 0


def test_stats_by_country_real(client):
    r = client.get("/api/v1/stats/by-country", headers=HEADERS)
    assert r.status_code == 200
    data = r.json()
    assert isinstance(data, list) and len(data) > 0
    assert all(c["nb_stations"] >= c["nb_airports"] for c in data)


def test_search_cities_real(client):
    r = client.get("/api/v1/search/cities?q=Pa", headers=HEADERS)
    assert r.status_code == 200
    assert isinstance(r.json(), list)


def test_monitoring_incidents_real(client):
    r = client.get("/api/v1/monitoring/incidents", headers=HEADERS)
    assert r.status_code == 200
    body = r.json()
    assert body["database"] == "connected"
    assert isinstance(body["incidents"], list)
    assert body["incident_count"] == sum(
        1 for i in body["incidents"] if i["severity"] != "ok"
    )


# ── Sécurité : l'auth s'applique bien sur la vraie stack ─────────────────────

def test_auth_required_on_real_stack(client):
    r = client.get("/api/v1/stations", headers={"Authorization": "Bearer wrong"})
    assert r.status_code == 401
