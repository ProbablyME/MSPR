from conftest import HEADERS, make_row

BASE = "/api/v1/emissions"


def _emission_row(**kwargs):
    defaults = dict(
        fact_id=1, transport_mode="train", route_train_id=1, vehicle_train_id=1,
        route_avion_id=None, vehicle_avion_id=None, co2_kg_passenger=8.5,
        dep_city="Paris", arr_city="Lyon",
    )
    defaults.update(kwargs)
    return make_row(**defaults)


# ---------------------------------------------------------------------------
# list_emissions
# ---------------------------------------------------------------------------

def test_list_emissions_empty(client, mock_db):
    mock_db.execute.return_value.fetchone.return_value = make_row(cnt=0)
    mock_db.execute.return_value.fetchall.return_value = []
    r = client.get(BASE, headers=HEADERS)
    assert r.status_code == 200
    assert r.json()["total"] == 0
    assert r.json()["data"] == []


def test_list_emissions_with_data(client, mock_db):
    mock_db.execute.return_value.fetchone.return_value = make_row(cnt=1)
    mock_db.execute.return_value.fetchall.return_value = [_emission_row()]
    r = client.get(BASE, headers=HEADERS)
    assert r.status_code == 200
    data = r.json()
    assert data["total"] == 1
    assert data["data"][0]["fact_id"] == 1
    assert data["data"][0]["transport_mode"] == "train"
    assert data["data"][0]["co2_kg_passenger"] == 8.5


def test_list_emissions_filter_transport_mode(client, mock_db):
    mock_db.execute.return_value.fetchone.return_value = make_row(cnt=1)
    mock_db.execute.return_value.fetchall.return_value = [
        _emission_row(transport_mode="avion", route_train_id=None, vehicle_train_id=None,
                      route_avion_id=2, vehicle_avion_id=3)
    ]
    r = client.get(f"{BASE}?transport_mode=avion", headers=HEADERS)
    assert r.status_code == 200
    assert r.json()["data"][0]["transport_mode"] == "avion"


def test_list_emissions_filter_cities(client, mock_db):
    mock_db.execute.return_value.fetchone.return_value = make_row(cnt=1)
    mock_db.execute.return_value.fetchall.return_value = [_emission_row()]
    r = client.get(f"{BASE}?dep_city=Paris&arr_city=Lyon", headers=HEADERS)
    assert r.status_code == 200


def test_list_emissions_pagination(client, mock_db):
    mock_db.execute.return_value.fetchone.return_value = make_row(cnt=200)
    mock_db.execute.return_value.fetchall.return_value = []
    r = client.get(f"{BASE}?page=3&per_page=20", headers=HEADERS)
    assert r.status_code == 200
    assert r.json()["page"] == 3
    assert r.json()["per_page"] == 20


# ---------------------------------------------------------------------------
# get_emission
# ---------------------------------------------------------------------------

def test_get_emission_found(client, mock_db):
    mock_db.execute.return_value.fetchone.return_value = _emission_row()
    r = client.get(f"{BASE}/1", headers=HEADERS)
    assert r.status_code == 200
    assert r.json()["fact_id"] == 1
    assert r.json()["dep_city"] == "Paris"


def test_get_emission_not_found(client, mock_db):
    mock_db.execute.return_value.fetchone.return_value = None
    r = client.get(f"{BASE}/9999", headers=HEADERS)
    assert r.status_code == 404


def test_get_emission_avion(client, mock_db):
    mock_db.execute.return_value.fetchone.return_value = _emission_row(
        transport_mode="avion", route_train_id=None, vehicle_train_id=None,
        route_avion_id=5, vehicle_avion_id=10,
    )
    r = client.get(f"{BASE}/2", headers=HEADERS)
    assert r.status_code == 200
    assert r.json()["transport_mode"] == "avion"
    assert r.json()["route_avion_id"] == 5
