from unittest.mock import MagicMock
from conftest import HEADERS, make_row

HEALTH_URL = "/api/v1/health"


def test_health_ok(client, mock_db):
    mock_db.execute.return_value.fetchone.return_value = make_row()
    r = client.get(HEALTH_URL, headers=HEADERS)
    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "ok"
    assert data["database"] == "connected"


def test_health_db_down(client, mock_db):
    mock_db.execute.side_effect = Exception("Connection refused")
    r = client.get(HEALTH_URL, headers=HEADERS)
    assert r.status_code == 503
    assert "inaccessible" in r.json()["detail"]
