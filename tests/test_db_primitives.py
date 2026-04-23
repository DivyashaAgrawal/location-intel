from __future__ import annotations

import pandas as pd

from src.caching import db


def test_migration_adds_columns_on_existing_db(temp_db):
    import sqlite3
    conn = sqlite3.connect(temp_db)
    cols = {r[1] for r in conn.execute("PRAGMA table_info(stores)").fetchall()}
    conn.close()
    assert "enriched_at" in cols
    assert "enrichment_source" in cols


def test_source_cache_roundtrip(temp_db):
    df = pd.DataFrame([{"brand": "Acme", "title": "One"}])
    db.set_source_cache("Acme", "Delhi", "google_places", df)
    got = db.get_source_cache("Acme", "Delhi", "google_places", ttl=3600)
    assert got is not None
    assert len(got) == 1


def test_upsert_store_assigns_stable_id_on_place_id(temp_db):
    rec = {"brand": "X", "place_id": "pid1", "title": "T", "city": "Delhi"}
    sid1 = db.upsert_store(rec)
    sid2 = db.upsert_store({**rec, "title": "T-updated"})
    assert sid1 == sid2
    row = db.get_stores_by_ids([sid1]).iloc[0]
    assert row["title"] == "T-updated"


def test_add_known_city_for_brand_is_idempotent(temp_db):
    db.upsert_brand_metadata("A", 10, "manual", 1.0, known_cities=["Delhi"])
    db.add_known_city_for_brand("A", "Delhi")
    db.add_known_city_for_brand("A", "Mumbai")
    meta = db.get_brand_metadata("A")
    assert sorted(meta["known_cities"]) == ["Delhi", "Mumbai"]


def test_cumulative_api_cost_accumulates(temp_db):
    db.log_api_call("google_places", brand="A", city="D")
    db.log_api_call("google_places", brand="A", city="M")
    db.log_api_call("serper", brand="A", city="D")
    cost = db.cumulative_api_cost()
    assert cost["total_calls"] == 3
    by_source = {r["source"]: r for r in cost["by_source"]}
    assert by_source["google_places"]["calls"] == 2
    assert by_source["serper"]["calls"] == 1
