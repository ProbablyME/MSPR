from conftest import HEADERS, make_row


def test_no_token_returns_403(client, mock_db):
    mock_db.execute.return_value.fetchone.return_value = make_row(cnt=0)
    mock_db.execute.return_value.fetchall.return_value = []
    response = client.get("/api/v1/stations")
    assert response.status_code == 403


def test_invalid_token_returns_401(client):
    response = client.get(
        "/api/v1/stations",
        headers={"Authorization": "Bearer mauvais-token"},
    )
    assert response.status_code == 401


def test_valid_token_returns_200(client, mock_db):
    mock_db.execute.return_value.fetchone.return_value = make_row(cnt=0)
    mock_db.execute.return_value.fetchall.return_value = []
    response = client.get("/api/v1/stations", headers=HEADERS)
    assert response.status_code == 200


def test_root_no_auth_required(client):
    """La racine / n'a pas de dépendance d'auth directe sur la route,
    mais l'app applique verify_token globalement."""
    response = client.get("/", headers=HEADERS)
    assert response.status_code == 200
    data = response.json()
    assert data["version"] == "2.0.0"
