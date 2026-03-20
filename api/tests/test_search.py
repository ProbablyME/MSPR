from conftest import HEADERS, make_row

CITIES_URL = "/api/v1/search/cities"
DEPARTURES_URL = "/api/v1/search/departures"
COMMON_URL = "/api/v1/locations/common-cities"


# ---------------------------------------------------------------------------
# search/cities
# ---------------------------------------------------------------------------

def test_search_cities_empty(client, mock_db):
    mock_db.execute.return_value.fetchall.return_value = []
    r = client.get(f"{CITIES_URL}?q=XX", headers=HEADERS)
    assert r.status_code == 200
    assert r.json() == []


def test_search_cities_with_results(client, mock_db):
    mock_db.execute.return_value.fetchall.return_value = [
        make_row(city="Paris", country_code="FR", has_train=True, has_airport=True),
        make_row(city="Parma", country_code="IT", has_train=True, has_airport=False),
    ]
    r = client.get(f"{CITIES_URL}?q=Par", headers=HEADERS)
    assert r.status_code == 200
    data = r.json()
    assert len(data) == 2
    assert data[0]["city"] == "Paris"
    assert data[0]["has_train"] is True
    assert data[0]["has_airport"] is True


def test_search_cities_with_limit(client, mock_db):
    mock_db.execute.return_value.fetchall.return_value = [
        make_row(city="Paris", country_code="FR", has_train=True, has_airport=True),
    ]
    r = client.get(f"{CITIES_URL}?q=Par&limit=5", headers=HEADERS)
    assert r.status_code == 200


# ---------------------------------------------------------------------------
# search/departures
# ---------------------------------------------------------------------------

def test_search_departures_found(client, mock_db):
    mock_db.execute.return_value.fetchall.return_value = [
        make_row(arr_city="Lyon", distance_km=465.0, train_co2_kg=8.5,
                 plane_co2_kg=85.0, greener_mode="train"),
        make_row(arr_city="Marseille", distance_km=770.0, train_co2_kg=12.0,
                 plane_co2_kg=None, greener_mode="train"),
    ]
    r = client.get(f"{DEPARTURES_URL}?dep_city=Paris", headers=HEADERS)
    assert r.status_code == 200
    data = r.json()
    assert len(data) == 2
    assert data[0]["arr_city"] == "Lyon"
    assert data[0]["greener_mode"] == "train"


def test_search_departures_not_found(client, mock_db):
    mock_db.execute.return_value.fetchall.return_value = []
    r = client.get(f"{DEPARTURES_URL}?dep_city=Inconnue", headers=HEADERS)
    assert r.status_code == 404


def test_search_departures_filter_night(client, mock_db):
    mock_db.execute.return_value.fetchall.return_value = [
        make_row(arr_city="Berlin", distance_km=1050.0, train_co2_kg=15.0,
                 plane_co2_kg=120.0, greener_mode="train"),
    ]
    r = client.get(f"{DEPARTURES_URL}?dep_city=Paris&train_type=nuit", headers=HEADERS)
    assert r.status_code == 200


def test_search_departures_filter_jour(client, mock_db):
    mock_db.execute.return_value.fetchall.return_value = [
        make_row(arr_city="Lyon", distance_km=465.0, train_co2_kg=8.5,
                 plane_co2_kg=85.0, greener_mode="train"),
    ]
    r = client.get(f"{DEPARTURES_URL}?dep_city=Paris&train_type=jour", headers=HEADERS)
    assert r.status_code == 200


def test_search_departures_filter_min_distance(client, mock_db):
    mock_db.execute.return_value.fetchall.return_value = [
        make_row(arr_city="Lyon", distance_km=465.0, train_co2_kg=8.5,
                 plane_co2_kg=85.0, greener_mode="train"),
    ]
    r = client.get(f"{DEPARTURES_URL}?dep_city=Paris&min_distance_km=300", headers=HEADERS)
    assert r.status_code == 200


# ---------------------------------------------------------------------------
# locations/common-cities
# ---------------------------------------------------------------------------

def test_common_cities_empty(client, mock_db):
    mock_db.execute.return_value.fetchall.return_value = []
    r = client.get(COMMON_URL, headers=HEADERS)
    assert r.status_code == 200
    assert r.json() == {"common_cities": []}


def test_common_cities_with_data(client, mock_db):
    mock_db.execute.return_value.fetchall.return_value = [
        make_row(city="Paris"),
        make_row(city="Lyon"),
    ]
    r = client.get(COMMON_URL, headers=HEADERS)
    assert r.status_code == 200
    assert r.json() == {"common_cities": ["Paris", "Lyon"]}
