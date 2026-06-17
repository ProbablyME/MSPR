"""Tests de la brique d'observabilité : export Prometheus et routes publiques."""

from conftest import HEADERS, make_row

METRICS_URL = "/metrics"


def test_metrics_endpoint_public(client):
    """/metrics doit répondre 200 SANS jeton (scrapé par Prometheus interne)."""
    r = client.get(METRICS_URL)
    assert r.status_code == 200
    body = r.text
    # Métrique par défaut de prometheus-fastapi-instrumentator
    assert "http_request_duration_seconds" in body


def test_metrics_exposes_request_counts(client, mock_db):
    """Après un appel métier, le compteur de requêtes doit être incrémenté."""
    mock_db.execute.return_value.fetchone.return_value = make_row(cnt=0)
    mock_db.execute.return_value.fetchall.return_value = []
    client.get("/api/v1/stations", headers=HEADERS)

    r = client.get(METRICS_URL)
    assert r.status_code == 200
    assert "http_request_duration_seconds_count" in r.text


def test_root_public_no_token(client):
    """La racine reste publique (health badge / probe de disponibilité)."""
    r = client.get("/")
    assert r.status_code == 200
    assert r.json()["version"] == "2.0.0"


def test_metrics_not_protected_by_invalid_token(client):
    """Un mauvais jeton ne doit pas bloquer /metrics (route hors auth)."""
    r = client.get(METRICS_URL, headers={"Authorization": "Bearer mauvais-token"})
    assert r.status_code == 200
