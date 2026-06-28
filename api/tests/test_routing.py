"""
Tests unitaires des fonctions PURES du moteur d'itinéraire ferroviaire
(`api.routers`) : distance haversine, plus court chemin (Dijkstra) et
construction/filtrage du graphe. Aucune base de données réelle : on injecte
directement des graphes en mémoire ou des lignes mockées.

Ces fonctions portent la logique métier centrale (comparaison train multi-segments
vs avion) et n'étaient pas couvertes par les tests d'endpoints mockés.
"""
from unittest.mock import MagicMock

import pytest

from api import routers
from api.routers import (
    MAX_SEGMENT_KM,
    TRAIN_CO2_PER_KM_FALLBACK,
    _haversine,
    _load_train_graph,
    _shortest_train,
)


def make_row(**kwargs):
    row = MagicMock()
    row._mapping = kwargs
    return row


# ── _haversine ────────────────────────────────────────────────────────────────

def test_haversine_zero_distance():
    assert _haversine(48.85, 2.35, 48.85, 2.35) == pytest.approx(0.0, abs=1e-6)


def test_haversine_paris_lyon():
    # Paris (48.857, 2.352) → Lyon (45.764, 4.835) ≈ 392 km
    d = _haversine(48.857, 2.352, 45.764, 4.835)
    assert 380 < d < 405


def test_haversine_symmetric():
    a = _haversine(48.857, 2.352, 45.764, 4.835)
    b = _haversine(45.764, 4.835, 48.857, 2.352)
    assert a == pytest.approx(b)


# ── _shortest_train (Dijkstra) ────────────────────────────────────────────────

def test_shortest_train_empty_inputs():
    assert _shortest_train({}, set(), set()) == (None, None, None)
    assert _shortest_train({}, {"A"}, set()) == (None, None, None)


def test_shortest_train_overlapping_dep_arr():
    # Une gare qui est à la fois départ et arrivée → trajet nul, écarté.
    assert _shortest_train({}, {"A"}, {"A"}) == (None, None, None)


def test_shortest_train_direct_segment():
    graph = {"A": [("B", 100.0, 2.5)]}
    dist, co2, chain = _shortest_train(graph, {"A"}, {"B"})
    assert dist == 100.0
    assert co2 == 2.5
    assert chain == ["A", "B"]


def test_shortest_train_picks_shortest_of_two_paths():
    # A→B→C (300 km) vs A→C direct (250 km) : Dijkstra doit choisir le direct.
    graph = {
        "A": [("B", 150.0, 4.0), ("C", 250.0, 6.0)],
        "B": [("C", 150.0, 4.0)],
    }
    dist, co2, chain = _shortest_train(graph, {"A"}, {"C"})
    assert dist == 250.0
    assert chain == ["A", "C"]


def test_shortest_train_multi_segment_when_only_path():
    graph = {
        "A": [("B", 150.0, 4.0)],
        "B": [("C", 150.0, 3.0)],
    }
    dist, co2, chain = _shortest_train(graph, {"A"}, {"C"})
    assert dist == 300.0
    assert co2 == pytest.approx(7.0)
    assert chain == ["A", "B", "C"]


def test_shortest_train_no_route_returns_none():
    graph = {"A": [("B", 100.0, 2.0)]}
    assert _shortest_train(graph, {"A"}, {"Z"}) == (None, None, None)


def test_shortest_train_max_dist_prunes():
    # max_dist borne l'EXPANSION : un nœud atteint au-delà du seuil n'est plus
    # étendu. Ici A→B→C→D = 450 km ; borné à 200, C (300 km) n'est pas étendu
    # vers D → aucun itinéraire trouvé.
    graph = {
        "A": [("B", 150.0, 4.0)],
        "B": [("C", 150.0, 3.0)],
        "C": [("D", 150.0, 3.0)],
    }
    assert _shortest_train(graph, {"A"}, {"D"}, max_dist=200.0) == (None, None, None)


# ── _load_train_graph (construction + filtrage des aberrations) ────────────────

@pytest.fixture(autouse=True)
def _reset_graph_cache():
    """Le graphe est mis en cache au niveau module : on réinitialise avant/après."""
    routers._TRAIN_GRAPH = None
    yield
    routers._TRAIN_GRAPH = None


def _graph_db(rows):
    db = MagicMock()
    db.execute.return_value.fetchall.return_value = rows
    return db


def test_load_graph_builds_edge_with_recomputed_distance():
    rows = [make_row(
        dep_station_id="A", arr_station_id="B", distance_km=390.0,  # ≈ distance réelle
        dlat=48.857, dlon=2.352, alat=45.764, alon=4.835,  # ~392 km réels
        co2_kg_passenger=5.0,
    )]
    graph = _load_train_graph(_graph_db(rows))
    assert "A" in graph
    arr, dist, co2 = graph["A"][0]
    assert arr == "B"
    assert co2 == 5.0
    assert 380 < dist < 405  # distance RECALCULÉE depuis les coordonnées, pas le 390 stocké


def test_load_graph_drops_segment_over_max_km():
    # Coordonnées Paris → Athènes (~2000 km) : > MAX_SEGMENT_KM → écarté.
    rows = [make_row(
        dep_station_id="A", arr_station_id="B", distance_km=None,
        dlat=48.857, dlon=2.352, alat=37.983, alon=23.727,
        co2_kg_passenger=None,
    )]
    graph = _load_train_graph(_graph_db(rows))
    assert graph.get("A", []) == []
    assert _haversine(48.857, 2.352, 37.983, 23.727) > MAX_SEGMENT_KM


def test_load_graph_uses_fallback_co2_when_missing():
    rows = [make_row(
        dep_station_id="A", arr_station_id="B", distance_km=None,
        dlat=48.857, dlon=2.352, alat=48.0, alon=2.4,  # court tronçon
        co2_kg_passenger=None,
    )]
    graph = _load_train_graph(_graph_db(rows))
    arr, dist, co2 = graph["A"][0]
    assert co2 == pytest.approx(dist * TRAIN_CO2_PER_KM_FALLBACK)


def test_load_graph_drops_stored_distance_divergence():
    # Distance réelle ~392 km mais distance stockée 5 km (collision stop_id) → écarté.
    rows = [make_row(
        dep_station_id="A", arr_station_id="B", distance_km=5.0,
        dlat=48.857, dlon=2.352, alat=45.764, alon=4.835,
        co2_kg_passenger=5.0,
    )]
    graph = _load_train_graph(_graph_db(rows))
    assert graph.get("A", []) == []


def test_load_graph_cached_between_calls():
    rows = [make_row(
        dep_station_id="A", arr_station_id="B", distance_km=None,
        dlat=48.857, dlon=2.352, alat=48.0, alon=2.4, co2_kg_passenger=1.0,
    )]
    db = _graph_db(rows)
    g1 = _load_train_graph(db)
    g2 = _load_train_graph(db)
    assert g1 is g2
    # Une seule requête SQL malgré deux appels (cache module).
    assert db.execute.call_count == 1
