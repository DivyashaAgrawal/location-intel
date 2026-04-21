from __future__ import annotations

import os
import tempfile

import pytest


@pytest.fixture
def temp_db(monkeypatch):
    """Fresh SQLite file per test, wired into src.core.db via env var."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    monkeypatch.setenv("LOCATION_INTEL_DB_PATH", path)

    from src.core import db as _db

    monkeypatch.setattr(_db, "DEFAULT_DB_PATH", path)
    _db.init_db()
    try:
        yield path
    finally:
        try:
            os.remove(path)
        except OSError:
            pass


@pytest.fixture
def no_api_keys(monkeypatch):
    """Ensure Google Places / Serper adapters short-circuit."""
    monkeypatch.setenv("GOOGLE_PLACES_API_KEY", "")
    monkeypatch.setenv("SERPER_API_KEY", "")
    from src.core import config
    monkeypatch.setattr(config, "GOOGLE_PLACES_API_KEY", "")
    monkeypatch.setattr(config, "SERPER_API_KEY", "")
