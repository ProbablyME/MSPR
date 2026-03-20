from conftest import HEADERS, make_row

BASE = "/api/v1/stations"

# ---------------------------------------------------------------------------
# list_stations
# ---------------------------------------------------------------------------

def test_list_stations_empty(client, mock_db):
    mock_db.execute.return_value.fetchone.return_value = make_row(cnt=0)
    mock_db.execute.return_value.fetchall.return_value = []
    r = client.get(BASE, headers=HEADERS)
    assert r.status_code == 200
    data = r.json()
    assert data["total"] == 0
    assert data["data"] == []
    assert data["page"] == 1


def test_list_stations_with_data(client, mock_db):
    mock_db.execute.return_value.fetchone.return_value = make_row(cnt=2)
    mock_db.execute.return_value.fetchall.return_value = [
        make_row(
            station_id="FR-001", station_name="Paris Gare du Nord", city="Paris",
            country_code="FR", latitude=48.88, longitude=2.35,
            is_airport=False, source_id=5,
        ),
        make_row(
            station_id="FR-002", station_name="Lyon Part-Dieu", city="Lyon",
            country_code="FR", latitude=45.76, longitude=4.86,
            is_airport=False, source_id=5,
        ),
    ]
    r = client.get(BASE, headers=HEADERS)
    assert r.status_code == 200
    data = r.json()
    assert data["total"] == 2
    assert len(data["data"]) == 2
    assert data["data"][0]["station_id"] == "FR-001"


def test_list_stations_filter_country(client, mock_db):
    mock_db.execute.return_value.fetchone.return_value = make_row(cnt=1)
    mock_db.execute.return_value.fetchall.return_value = [
        make_row(
            station_id="DE-001", station_name="Berlin Hbf", city="Berlin",
            country_code="DE", latitude=52.52, longitude=13.37,
            is_airport=False, source_id=5,
        )
    ]
    r = client.get(f"{BASE}?country_code=DE", headers=HEADERS)
    assert r.status_code == 200
    assert r.json()["data"][0]["country_code"] == "DE"


def test_list_stations_filter_airport(client, mock_db):
    mock_db.execute.return_value.fetchone.return_value = make_row(cnt=1)
    mock_db.execute.return_value.fetchall.return_value = [
        make_row(
            station_id="CDG", station_name="Paris CDG", city="Paris",
            country_code="FR", latitude=49.01, longitude=2.55,
            is_airport=True, source_id=3,
        )
    ]
    r = client.get(f"{BASE}?is_airport=true", headers=HEADERS)
    assert r.status_code == 200
    assert r.json()["data"][0]["is_airport"] is True


def test_list_stations_filter_city(client, mock_db):
    mock_db.execute.return_value.fetchone.return_value = make_row(cnt=1)
    mock_db.execute.return_value.fetchall.return_value = [
        make_row(
            station_id="FR-001", station_name="Paris Gare du Nord", city="Paris",
            country_code="FR", latitude=48.88, longitude=2.35,
            is_airport=False, source_id=5,
        )
    ]
    r = client.get(f"{BASE}?city=Paris", headers=HEADERS)
    assert r.status_code == 200


def test_list_stations_filter_search(client, mock_db):
    mock_db.execute.return_value.fetchone.return_value = make_row(cnt=1)
    mock_db.execute.return_value.fetchall.return_value = [
        make_row(
            station_id="FR-001", station_name="Paris Gare du Nord", city="Paris",
            country_code="FR", latitude=48.88, longitude=2.35,
            is_airport=False, source_id=5,
        )
    ]
    r = client.get(f"{BASE}?search=paris", headers=HEADERS)
    assert r.status_code == 200


def test_list_stations_pagination(client, mock_db):
    mock_db.execute.return_value.fetchone.return_value = make_row(cnt=100)
    mock_db.execute.return_value.fetchall.return_value = []
    r = client.get(f"{BASE}?page=2&per_page=10", headers=HEADERS)
    assert r.status_code == 200
    data = r.json()
    assert data["page"] == 2
    assert data["per_page"] == 10


# ---------------------------------------------------------------------------
# get_station
# ---------------------------------------------------------------------------

def test_get_station_found(client, mock_db):
    mock_db.execute.return_value.fetchone.return_value = make_row(
        station_id="FR-001", station_name="Paris Gare du Nord", city="Paris",
        country_code="FR", latitude=48.88, longitude=2.35,
        is_airport=False, source_id=5,
    )
    r = client.get(f"{BASE}/FR-001", headers=HEADERS)
    assert r.status_code == 200
    assert r.json()["station_id"] == "FR-001"


def test_get_station_not_found(client, mock_db):
    mock_db.execute.return_value.fetchone.return_value = None
    r = client.get(f"{BASE}/INEXISTANT", headers=HEADERS)
    assert r.status_code == 404
