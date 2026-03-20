from conftest import HEADERS, make_row

SOURCES_URL = "/api/v1/sources"
ETL_URL = "/api/v1/etl/runs"


def _source_row(**kwargs):
    defaults = dict(
        source_id=1, source_name="EASA CO2 Database", source_type="fichier_local",
        url="https://www.easa.europa.eu/", version="Issue 11",
        description="Base CO2 EASA", loaded_at="2026-03-07 18:43:11",
    )
    defaults.update(kwargs)
    return make_row(**defaults)


def _etl_row(**kwargs):
    defaults = dict(
        run_id=1, started_at="2026-03-07 18:00:00", finished_at="2026-03-07 18:43:00",
        duration_s=2580.0, mois_traites=12, airports_charges=500,
        routes_chargees=1200, co2_exact=900, co2_fallback=300,
        erreurs=0, statut="success",
    )
    defaults.update(kwargs)
    return make_row(**defaults)


# ---------------------------------------------------------------------------
# sources
# ---------------------------------------------------------------------------

def test_list_sources_empty(client, mock_db):
    mock_db.execute.return_value.fetchall.return_value = []
    r = client.get(SOURCES_URL, headers=HEADERS)
    assert r.status_code == 200
    assert r.json() == []


def test_list_sources_with_data(client, mock_db):
    mock_db.execute.return_value.fetchall.return_value = [_source_row()]
    r = client.get(SOURCES_URL, headers=HEADERS)
    assert r.status_code == 200
    data = r.json()
    assert data[0]["source_name"] == "EASA CO2 Database"
    assert data[0]["source_type"] == "fichier_local"


def test_get_source_found(client, mock_db):
    mock_db.execute.return_value.fetchone.return_value = _source_row()
    r = client.get(f"{SOURCES_URL}/1", headers=HEADERS)
    assert r.status_code == 200
    assert r.json()["source_id"] == 1


def test_get_source_not_found(client, mock_db):
    mock_db.execute.return_value.fetchone.return_value = None
    r = client.get(f"{SOURCES_URL}/9999", headers=HEADERS)
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# etl/runs
# ---------------------------------------------------------------------------

def test_list_etl_runs_empty(client, mock_db):
    mock_db.execute.return_value.fetchall.return_value = []
    r = client.get(ETL_URL, headers=HEADERS)
    assert r.status_code == 200
    assert r.json() == []


def test_list_etl_runs_with_data(client, mock_db):
    mock_db.execute.return_value.fetchall.return_value = [_etl_row()]
    r = client.get(ETL_URL, headers=HEADERS)
    assert r.status_code == 200
    data = r.json()
    assert data[0]["run_id"] == 1
    assert data[0]["statut"] == "success"


def test_list_etl_runs_with_limit(client, mock_db):
    mock_db.execute.return_value.fetchall.return_value = [_etl_row()]
    r = client.get(f"{ETL_URL}?limit=5", headers=HEADERS)
    assert r.status_code == 200


def test_get_latest_etl_run_found(client, mock_db):
    mock_db.execute.return_value.fetchone.return_value = _etl_row()
    r = client.get(f"{ETL_URL}/latest", headers=HEADERS)
    assert r.status_code == 200
    assert r.json()["statut"] == "success"


def test_get_latest_etl_run_not_found(client, mock_db):
    mock_db.execute.return_value.fetchone.return_value = None
    r = client.get(f"{ETL_URL}/latest", headers=HEADERS)
    assert r.status_code == 404
