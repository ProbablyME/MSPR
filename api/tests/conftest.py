from unittest.mock import MagicMock
import pytest
from fastapi.testclient import TestClient

from api.main import app
from api.database import get_db

# Token par défaut défini dans config.py
TOKEN = "my-secret-token-123"
HEADERS = {"Authorization": f"Bearer {TOKEN}"}


def make_row(**kwargs):
    """Crée un mock de ligne SQLAlchemy avec un _mapping dict-like."""
    row = MagicMock()
    row._mapping = kwargs
    return row


@pytest.fixture
def mock_db():
    db = MagicMock()
    db.execute.return_value.fetchone.return_value = None
    db.execute.return_value.fetchall.return_value = []
    return db


@pytest.fixture
def client(mock_db):
    app.dependency_overrides[get_db] = lambda: mock_db
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()
