"""Tests de l'itinéraire train multi-segments /compare/journey."""

import api.routers as routers
from conftest import HEADERS, make_row

JOURNEY_URL = "/api/v1/compare/journey"


def setup_function(_):
    # Réinitialise le cache du graphe entre les tests
    routers._TRAIN_GRAPH = None


def test_journey_404_when_no_data(client, mock_db):
    # Graphe vide + pas de stations + avion absent -> 404
    mock_db.execute.return_value.fetchall.return_value = []
    mock_db.execute.return_value.fetchone.return_value = make_row(plane_co2=None, dist=None)

    r = client.get(JOURNEY_URL, params={"dep_city": "Nulleville", "arr_city": "Videbourg"}, headers=HEADERS)
    assert r.status_code == 404


def test_journey_plane_only(client, mock_db):
    # Pas de tronçon train, mais un vol direct existe -> found=False, greener=avion
    mock_db.execute.return_value.fetchall.return_value = []
    mock_db.execute.return_value.fetchone.return_value = make_row(plane_co2=4.5, dist=900.0)

    r = client.get(JOURNEY_URL, params={"dep_city": "Paris", "arr_city": "Berlin"}, headers=HEADERS)
    assert r.status_code == 200
    data = r.json()
    assert data["found"] is False
    assert data["plane_co2_kg"] == 4.5
    assert data["greener_mode"] == "avion"
    assert data["train_co2_kg"] is None
