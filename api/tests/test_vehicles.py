from conftest import HEADERS, make_row

TRAIN_BASE = "/api/v1/vehicles/train"
AVION_BASE = "/api/v1/vehicles/avion"


def _train_vehicle(**kwargs):
    defaults = dict(vehicle_train_id=1, label="TGV Duplex", co2_per_km=0.004, service_type="grande_vitesse")
    defaults.update(kwargs)
    return make_row(**defaults)


def _avion_vehicle(**kwargs):
    defaults = dict(
        vehicle_avion_id=1, label="A320-251N", co2_per_km=0.728,
        service_type="court_moyen_courrier", icao_typecode="A20N",
        mtom_kg=79000, num_engines=2, manufacturer="Airbus S.A.S.",
    )
    defaults.update(kwargs)
    return make_row(**defaults)


# ---------------------------------------------------------------------------
# VEHICLES TRAIN
# ---------------------------------------------------------------------------

def test_list_vehicles_train_empty(client, mock_db):
    mock_db.execute.return_value.fetchall.return_value = []
    r = client.get(TRAIN_BASE, headers=HEADERS)
    assert r.status_code == 200
    assert r.json() == []


def test_list_vehicles_train_with_data(client, mock_db):
    mock_db.execute.return_value.fetchall.return_value = [_train_vehicle()]
    r = client.get(TRAIN_BASE, headers=HEADERS)
    assert r.status_code == 200
    data = r.json()
    assert len(data) == 1
    assert data[0]["vehicle_train_id"] == 1
    assert data[0]["label"] == "TGV Duplex"


def test_list_vehicles_train_filter_service_type(client, mock_db):
    mock_db.execute.return_value.fetchall.return_value = [_train_vehicle()]
    r = client.get(f"{TRAIN_BASE}?service_type=grande_vitesse", headers=HEADERS)
    assert r.status_code == 200


# ---------------------------------------------------------------------------
# VEHICLES AVION
# ---------------------------------------------------------------------------

def test_list_vehicles_avion_empty(client, mock_db):
    mock_db.execute.return_value.fetchall.return_value = []
    r = client.get(AVION_BASE, headers=HEADERS)
    assert r.status_code == 200
    assert r.json() == []


def test_list_vehicles_avion_with_data(client, mock_db):
    mock_db.execute.return_value.fetchall.return_value = [_avion_vehicle()]
    r = client.get(AVION_BASE, headers=HEADERS)
    assert r.status_code == 200
    data = r.json()
    assert data[0]["label"] == "A320-251N"
    assert data[0]["service_type"] == "court_moyen_courrier"


def test_list_vehicles_avion_filter_service_type(client, mock_db):
    mock_db.execute.return_value.fetchall.return_value = [_avion_vehicle()]
    r = client.get(f"{AVION_BASE}?service_type=court_moyen_courrier", headers=HEADERS)
    assert r.status_code == 200


def test_list_vehicles_avion_filter_manufacturer(client, mock_db):
    mock_db.execute.return_value.fetchall.return_value = [_avion_vehicle()]
    r = client.get(f"{AVION_BASE}?manufacturer=Airbus", headers=HEADERS)
    assert r.status_code == 200


def test_list_vehicles_avion_filter_icao(client, mock_db):
    mock_db.execute.return_value.fetchall.return_value = [_avion_vehicle()]
    r = client.get(f"{AVION_BASE}?icao_typecode=A20N", headers=HEADERS)
    assert r.status_code == 200


def test_get_vehicle_avion_found(client, mock_db):
    mock_db.execute.return_value.fetchone.return_value = _avion_vehicle()
    r = client.get(f"{AVION_BASE}/1", headers=HEADERS)
    assert r.status_code == 200
    assert r.json()["vehicle_avion_id"] == 1
    assert r.json()["manufacturer"] == "Airbus S.A.S."


def test_get_vehicle_avion_not_found(client, mock_db):
    mock_db.execute.return_value.fetchone.return_value = None
    r = client.get(f"{AVION_BASE}/9999", headers=HEADERS)
    assert r.status_code == 404


def test_get_vehicle_avion_null_optional_fields(client, mock_db):
    mock_db.execute.return_value.fetchone.return_value = _avion_vehicle(
        icao_typecode=None, mtom_kg=None, num_engines=None, manufacturer=None
    )
    r = client.get(f"{AVION_BASE}/1", headers=HEADERS)
    assert r.status_code == 200
    assert r.json()["icao_typecode"] is None
