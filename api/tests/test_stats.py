from conftest import HEADERS, make_row

GREENER_URL = "/api/v1/ranking/greener-routes"
LONGEST_URL = "/api/v1/ranking/longest-routes"
NETWORK_URL = "/api/v1/stats/network"
COUNTRY_URL = "/api/v1/stats/by-country"
NIGHT_URL = "/api/v1/stats/night-vs-day"


def _ranking_row(**kwargs):
    defaults = dict(
        dep_city="Paris", arr_city="Lyon",
        train_co2_kg=8.5, plane_co2_kg=85.0,
        savings_kg=76.5, savings_percent=90.0,
    )
    defaults.update(kwargs)
    return make_row(**defaults)


def _longest_row(**kwargs):
    defaults = dict(dep_city="Paris", arr_city="Berlin", distance_km=1050.0, transport_mode="train")
    defaults.update(kwargs)
    return make_row(**defaults)


# ---------------------------------------------------------------------------
# ranking/greener-routes
# ---------------------------------------------------------------------------

def test_ranking_greener_found(client, mock_db):
    mock_db.execute.return_value.fetchall.return_value = [_ranking_row()]
    r = client.get(GREENER_URL, headers=HEADERS)
    assert r.status_code == 200
    data = r.json()
    assert len(data) == 1
    assert data[0]["savings_percent"] == 90.0


def test_ranking_greener_not_found(client, mock_db):
    mock_db.execute.return_value.fetchall.return_value = []
    r = client.get(GREENER_URL, headers=HEADERS)
    assert r.status_code == 404


def test_ranking_greener_with_limit(client, mock_db):
    mock_db.execute.return_value.fetchall.return_value = [_ranking_row()] * 5
    r = client.get(f"{GREENER_URL}?limit=5", headers=HEADERS)
    assert r.status_code == 200
    assert len(r.json()) == 5


# ---------------------------------------------------------------------------
# ranking/longest-routes
# ---------------------------------------------------------------------------

def test_ranking_longest_all_modes(client, mock_db):
    mock_db.execute.return_value.fetchall.return_value = [
        _longest_row(transport_mode="avion", distance_km=9000.0),
        _longest_row(transport_mode="train", distance_km=1050.0),
    ]
    r = client.get(LONGEST_URL, headers=HEADERS)
    assert r.status_code == 200
    assert len(r.json()) == 2


def test_ranking_longest_train_only(client, mock_db):
    mock_db.execute.return_value.fetchall.return_value = [_longest_row()]
    r = client.get(f"{LONGEST_URL}?transport_mode=train", headers=HEADERS)
    assert r.status_code == 200
    assert r.json()[0]["transport_mode"] == "train"


def test_ranking_longest_avion_only(client, mock_db):
    mock_db.execute.return_value.fetchall.return_value = [
        _longest_row(transport_mode="avion", distance_km=9000.0)
    ]
    r = client.get(f"{LONGEST_URL}?transport_mode=avion", headers=HEADERS)
    assert r.status_code == 200
    assert r.json()[0]["transport_mode"] == "avion"


def test_ranking_longest_invalid_mode(client, mock_db):
    """Un transport_mode invalide vide `parts` → 400."""
    r = client.get(f"{LONGEST_URL}?transport_mode=velo", headers=HEADERS)
    assert r.status_code == 400


# ---------------------------------------------------------------------------
# stats/network
# ---------------------------------------------------------------------------

def test_stats_network(client, mock_db):
    mock_db.execute.return_value.fetchone.return_value = make_row(
        total_train_routes=50, total_plane_routes=30, total_comparable_routes=20,
        avg_train_co2_kg=10.5, avg_plane_co2_kg=95.0,
        avg_savings_kg=84.5, avg_savings_percent=88.9,
    )
    r = client.get(NETWORK_URL, headers=HEADERS)
    assert r.status_code == 200
    data = r.json()
    assert data["total_train_routes"] == 50
    assert data["avg_savings_percent"] == 88.9


def test_stats_network_no_data(client, mock_db):
    mock_db.execute.return_value.fetchone.return_value = None
    r = client.get(NETWORK_URL, headers=HEADERS)
    assert r.status_code == 500


# ---------------------------------------------------------------------------
# stats/by-country
# ---------------------------------------------------------------------------

def test_stats_by_country_all(client, mock_db):
    mock_db.execute.return_value.fetchall.return_value = [
        make_row(country_code="FR", nb_stations=500, nb_airports=20,
                 nb_train_stations=480, nb_train_routes=300, nb_avion_routes=150),
    ]
    r = client.get(COUNTRY_URL, headers=HEADERS)
    assert r.status_code == 200
    assert r.json()[0]["country_code"] == "FR"


def test_stats_by_country_filter(client, mock_db):
    mock_db.execute.return_value.fetchall.return_value = [
        make_row(country_code="DE", nb_stations=400, nb_airports=15,
                 nb_train_stations=385, nb_train_routes=250, nb_avion_routes=100),
    ]
    r = client.get(f"{COUNTRY_URL}?country_code=DE", headers=HEADERS)
    assert r.status_code == 200
    assert r.json()[0]["country_code"] == "DE"


# ---------------------------------------------------------------------------
# stats/night-vs-day
# ---------------------------------------------------------------------------

def test_stats_night_vs_day(client, mock_db):
    mock_db.execute.return_value.fetchall.return_value = [
        make_row(type="jour", nb_routes=200, avg_distance_km=450.0,
                 avg_co2_kg_passenger=9.0, total_routes_with_emission=180),
        make_row(type="nuit", nb_routes=50, avg_distance_km=900.0,
                 avg_co2_kg_passenger=12.0, total_routes_with_emission=40),
    ]
    r = client.get(NIGHT_URL, headers=HEADERS)
    assert r.status_code == 200
    data = r.json()
    assert len(data) == 2
    types = {d["type"] for d in data}
    assert types == {"jour", "nuit"}


def test_stats_night_vs_day_empty(client, mock_db):
    mock_db.execute.return_value.fetchall.return_value = []
    r = client.get(NIGHT_URL, headers=HEADERS)
    assert r.status_code == 200
    assert r.json() == []
