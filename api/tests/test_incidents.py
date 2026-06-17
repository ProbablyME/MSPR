"""Tests de la vue de supervision /monitoring/incidents."""

from conftest import HEADERS, make_row

URL = "/api/v1/monitoring/incidents"


def test_incidents_all_ok(client, mock_db):
    mock_db.execute.return_value.fetchone.return_value = make_row(
        statut="succes", erreurs=0,
        co2_invalid=0, avion_no_co2=0, no_gps=0, no_city=0, train_orphans=0,
    )
    r = client.get(URL, headers=HEADERS)
    assert r.status_code == 200
    data = r.json()
    assert data["api_status"] == "ok"
    assert data["database"] == "connected"
    assert data["incident_count"] == 0
    assert any(i["label"] == "Connexion base de données" for i in data["incidents"])


def test_incidents_detects_problems(client, mock_db):
    mock_db.execute.return_value.fetchone.return_value = make_row(
        statut="erreur", erreurs=42,
        co2_invalid=500, avion_no_co2=2000, no_gps=10, no_city=0, train_orphans=5000,
    )
    r = client.get(URL, headers=HEADERS)
    assert r.status_code == 200
    data = r.json()
    assert data["incident_count"] >= 3
    sev = {i["label"]: i["severity"] for i in data["incidents"]}
    assert sev["Dernier run ETL"] == "error"
    assert sev["Faits CO₂ nuls ou négatifs"] == "error"
