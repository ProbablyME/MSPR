from conftest import HEADERS, make_row

TRAIN_BASE = "/api/v1/routes/train"
AVION_BASE = "/api/v1/routes/avion"


def _train_row(**kwargs):
    defaults = dict(
        route_train_id=1, dep_station_id="FR-001", dep_station_name="Paris Gare du Nord",
        dep_city="Paris", arr_station_id="FR-002", arr_station_name="Lyon Part-Dieu",
        arr_city="Lyon", distance_km=465.0, is_night_train=False,
    )
    defaults.update(kwargs)
    return make_row(**defaults)


def _avion_row(**kwargs):
    defaults = dict(
        route_avion_id=1, dep_station_id="CDG", dep_station_name="Paris CDG",
        dep_city="Paris", arr_station_id="LYS", arr_station_name="Lyon Saint-Exupery",
        arr_city="Lyon", distance_km=392.0, dominant_typecode="A320",
        co2_total_kg=1500.0,
    )
    defaults.update(kwargs)
    return make_row(**defaults)


# ---------------------------------------------------------------------------
# ROUTES TRAIN
# ---------------------------------------------------------------------------

def test_list_routes_train_empty(client, mock_db):
    mock_db.execute.return_value.fetchone.return_value = make_row(cnt=0)
    mock_db.execute.return_value.fetchall.return_value = []
    r = client.get(TRAIN_BASE, headers=HEADERS)
    assert r.status_code == 200
    assert r.json()["total"] == 0


def test_list_routes_train_with_data(client, mock_db):
    mock_db.execute.return_value.fetchone.return_value = make_row(cnt=1)
    mock_db.execute.return_value.fetchall.return_value = [_train_row()]
    r = client.get(TRAIN_BASE, headers=HEADERS)
    assert r.status_code == 200
    data = r.json()
    assert data["total"] == 1
    assert data["data"][0]["route_train_id"] == 1
    assert data["data"][0]["is_night_train"] is False


def test_list_routes_train_filter_night(client, mock_db):
    mock_db.execute.return_value.fetchone.return_value = make_row(cnt=1)
    mock_db.execute.return_value.fetchall.return_value = [_train_row(is_night_train=True)]
    r = client.get(f"{TRAIN_BASE}?is_night_train=true", headers=HEADERS)
    assert r.status_code == 200
    assert r.json()["data"][0]["is_night_train"] is True


def test_list_routes_train_filter_country(client, mock_db):
    mock_db.execute.return_value.fetchone.return_value = make_row(cnt=1)
    mock_db.execute.return_value.fetchall.return_value = [_train_row()]
    r = client.get(f"{TRAIN_BASE}?country_code=FR", headers=HEADERS)
    assert r.status_code == 200


def test_list_routes_train_filter_cities(client, mock_db):
    mock_db.execute.return_value.fetchone.return_value = make_row(cnt=1)
    mock_db.execute.return_value.fetchall.return_value = [_train_row()]
    r = client.get(f"{TRAIN_BASE}?dep_city=Paris&arr_city=Lyon", headers=HEADERS)
    assert r.status_code == 200


def test_list_routes_train_filter_min_distance(client, mock_db):
    mock_db.execute.return_value.fetchone.return_value = make_row(cnt=1)
    mock_db.execute.return_value.fetchall.return_value = [_train_row()]
    r = client.get(f"{TRAIN_BASE}?min_distance_km=100", headers=HEADERS)
    assert r.status_code == 200


def test_get_route_train_found(client, mock_db):
    mock_db.execute.return_value.fetchone.return_value = _train_row()
    r = client.get(f"{TRAIN_BASE}/1", headers=HEADERS)
    assert r.status_code == 200
    assert r.json()["route_train_id"] == 1
    assert r.json()["dep_city"] == "Paris"


def test_get_route_train_not_found(client, mock_db):
    mock_db.execute.return_value.fetchone.return_value = None
    r = client.get(f"{TRAIN_BASE}/9999", headers=HEADERS)
    assert r.status_code == 404


def test_get_route_train_no_distance(client, mock_db):
    """distance_km peut être None (NULL en base)."""
    mock_db.execute.return_value.fetchone.return_value = _train_row(distance_km=None)
    r = client.get(f"{TRAIN_BASE}/1", headers=HEADERS)
    assert r.status_code == 200
    assert r.json()["distance_km"] is None


# ---------------------------------------------------------------------------
# ROUTES AVION
# ---------------------------------------------------------------------------

def test_list_routes_avion_empty(client, mock_db):
    mock_db.execute.return_value.fetchone.return_value = make_row(cnt=0)
    mock_db.execute.return_value.fetchall.return_value = []
    r = client.get(AVION_BASE, headers=HEADERS)
    assert r.status_code == 200
    assert r.json()["total"] == 0


def test_list_routes_avion_with_data(client, mock_db):
    mock_db.execute.return_value.fetchone.return_value = make_row(cnt=1)
    mock_db.execute.return_value.fetchall.return_value = [_avion_row()]
    r = client.get(AVION_BASE, headers=HEADERS)
    assert r.status_code == 200
    assert r.json()["data"][0]["route_avion_id"] == 1


def test_list_routes_avion_filters(client, mock_db):
    mock_db.execute.return_value.fetchone.return_value = make_row(cnt=1)
    mock_db.execute.return_value.fetchall.return_value = [_avion_row()]
    r = client.get(f"{AVION_BASE}?dep_city=Paris&arr_city=Lyon&country_code=FR&min_distance_km=100", headers=HEADERS)
    assert r.status_code == 200


def test_get_route_avion_found(client, mock_db):
    mock_db.execute.return_value.fetchone.return_value = _avion_row()
    r = client.get(f"{AVION_BASE}/1", headers=HEADERS)
    assert r.status_code == 200
    assert r.json()["route_avion_id"] == 1


def test_get_route_avion_not_found(client, mock_db):
    mock_db.execute.return_value.fetchone.return_value = None
    r = client.get(f"{AVION_BASE}/9999", headers=HEADERS)
    assert r.status_code == 404


def test_get_route_avion_null_fields(client, mock_db):
    mock_db.execute.return_value.fetchone.return_value = _avion_row(
        distance_km=None, co2_total_kg=None, dominant_typecode=None
    )
    r = client.get(f"{AVION_BASE}/1", headers=HEADERS)
    assert r.status_code == 200
    assert r.json()["distance_km"] is None
    assert r.json()["co2_total_kg"] is None
