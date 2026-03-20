from conftest import HEADERS, make_row

CITIES_URL = "/api/v1/compare/cities"
WINNER_URL = "/api/v1/compare/winner"
PASSENGERS_URL = "/api/v1/compare/passengers"


def _compare_row(mode="train", dep_city="Paris", arr_city="Lyon",
                 distance_km=465.0, co2_per_km=0.004, co2_kg_passenger=8.5):
    return make_row(
        mode=mode, dep_city=dep_city, arr_city=arr_city,
        distance_km=distance_km, co2_per_km=co2_per_km, co2_kg_passenger=co2_kg_passenger,
    )


# ---------------------------------------------------------------------------
# compare/cities
# ---------------------------------------------------------------------------

def test_compare_cities_found(client, mock_db):
    mock_db.execute.return_value.fetchall.return_value = [
        _compare_row("train"), _compare_row("avion", co2_kg_passenger=85.0),
    ]
    r = client.get(f"{CITIES_URL}?dep_city=Paris&arr_city=Lyon", headers=HEADERS)
    assert r.status_code == 200
    data = r.json()
    assert len(data) == 2
    modes = {d["mode"] for d in data}
    assert modes == {"train", "avion"}


def test_compare_cities_not_found(client, mock_db):
    mock_db.execute.return_value.fetchall.return_value = []
    r = client.get(f"{CITIES_URL}?dep_city=Inconnue&arr_city=Nulle", headers=HEADERS)
    assert r.status_code == 404


def test_compare_cities_filter_night(client, mock_db):
    mock_db.execute.return_value.fetchall.return_value = [_compare_row("train")]
    r = client.get(f"{CITIES_URL}?dep_city=Paris&arr_city=Lyon&train_type=nuit", headers=HEADERS)
    assert r.status_code == 200


def test_compare_cities_filter_jour(client, mock_db):
    mock_db.execute.return_value.fetchall.return_value = [_compare_row("train")]
    r = client.get(f"{CITIES_URL}?dep_city=Paris&arr_city=Lyon&train_type=jour", headers=HEADERS)
    assert r.status_code == 200


def test_compare_cities_filter_min_distance(client, mock_db):
    mock_db.execute.return_value.fetchall.return_value = [_compare_row("train")]
    r = client.get(f"{CITIES_URL}?dep_city=Paris&arr_city=Lyon&min_distance_km=200", headers=HEADERS)
    assert r.status_code == 200


# ---------------------------------------------------------------------------
# compare/winner — train gagne
# ---------------------------------------------------------------------------

def test_compare_winner_train_wins(client, mock_db):
    mock_db.execute.return_value.fetchone.return_value = make_row(
        train_co2=8.5, plane_co2=85.0, dep_city="Paris", arr_city="Lyon"
    )
    r = client.get(f"{WINNER_URL}?dep_city=Paris&arr_city=Lyon", headers=HEADERS)
    assert r.status_code == 200
    data = r.json()
    assert data["greener_mode"] == "train"
    assert data["savings_kg"] > 0


def test_compare_winner_avion_wins(client, mock_db):
    """Cas improbable mais possible : avion < train en CO2."""
    mock_db.execute.return_value.fetchone.return_value = make_row(
        train_co2=100.0, plane_co2=50.0, dep_city="Paris", arr_city="NY"
    )
    r = client.get(f"{WINNER_URL}?dep_city=Paris&arr_city=NY", headers=HEADERS)
    assert r.status_code == 200
    assert r.json()["greener_mode"] == "avion"


def test_compare_winner_only_train(client, mock_db):
    """Seule la donnée train est disponible."""
    mock_db.execute.return_value.fetchone.return_value = make_row(
        train_co2=8.5, plane_co2=None, dep_city="Paris", arr_city="Lyon"
    )
    r = client.get(f"{WINNER_URL}?dep_city=Paris&arr_city=Lyon", headers=HEADERS)
    assert r.status_code == 200
    assert r.json()["greener_mode"] == "train"


def test_compare_winner_only_avion(client, mock_db):
    """Seule la donnée avion est disponible."""
    mock_db.execute.return_value.fetchone.return_value = make_row(
        train_co2=None, plane_co2=85.0, dep_city="Paris", arr_city="Lyon"
    )
    r = client.get(f"{WINNER_URL}?dep_city=Paris&arr_city=Lyon", headers=HEADERS)
    assert r.status_code == 200
    assert r.json()["greener_mode"] == "avion"


def test_compare_winner_not_found(client, mock_db):
    mock_db.execute.return_value.fetchone.return_value = make_row(
        train_co2=None, plane_co2=None, dep_city=None, arr_city=None
    )
    r = client.get(f"{WINNER_URL}?dep_city=Inconnue&arr_city=Nulle", headers=HEADERS)
    assert r.status_code == 404


def test_compare_winner_row_none(client, mock_db):
    mock_db.execute.return_value.fetchone.return_value = None
    r = client.get(f"{WINNER_URL}?dep_city=Inconnue&arr_city=Nulle", headers=HEADERS)
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# compare/passengers
# ---------------------------------------------------------------------------

def test_compare_passengers_found(client, mock_db):
    mock_db.execute.return_value.fetchall.return_value = [
        make_row(mode="train", dep_city="Paris", arr_city="Lyon", co2_per_passenger=8.5),
        make_row(mode="avion", dep_city="Paris", arr_city="Lyon", co2_per_passenger=85.0),
    ]
    r = client.get(f"{PASSENGERS_URL}?dep_city=Paris&arr_city=Lyon&passengers=4", headers=HEADERS)
    assert r.status_code == 200
    data = r.json()
    assert len(data) == 2
    train = next(d for d in data if d["mode"] == "train")
    assert train["passengers"] == 4
    assert train["total_co2_kg"] == round(8.5 * 4, 3)
    assert "equivalent_trees" in train
    assert "equivalent_car_km" in train


def test_compare_passengers_not_found(client, mock_db):
    mock_db.execute.return_value.fetchall.return_value = []
    r = client.get(f"{PASSENGERS_URL}?dep_city=Inconnue&arr_city=Nulle&passengers=2", headers=HEADERS)
    assert r.status_code == 404
