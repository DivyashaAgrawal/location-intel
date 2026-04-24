from __future__ import annotations

import pandas as pd
import pytest

from src.cache import db
from src.cache import manager as cache_manager


@pytest.fixture
def stub_scraper(monkeypatch):
    """Replace scrape_brand_stores with a deterministic national footprint."""
    def fake_scrape(brand, cities):
        return pd.DataFrame([
            {"brand": brand, "place_id": "del-1", "city": "Delhi",
             "title": "Delhi 1", "latitude": 28.61, "longitude": 77.2},
            {"brand": brand, "place_id": "del-2", "city": "Delhi",
             "title": "Delhi 2", "latitude": 28.62, "longitude": 77.21},
            {"brand": brand, "place_id": "mum-1", "city": "Mumbai",
             "title": "Mumbai 1", "latitude": 19.07, "longitude": 72.87},
            {"brand": brand, "place_id": "blr-1", "city": "Bangalore",
             "title": "Bangalore 1", "latitude": 12.97, "longitude": 77.59},
        ])
    from src.fetchers import brand_scraper
    monkeypatch.setattr(brand_scraper, "scrape_brand_stores", fake_scrape)
    monkeypatch.setitem(
        brand_scraper.BRAND_REGISTRY,
        "Acme",
        {
            "extraction_method": "html",
            "store_locator_url": "http://example.invalid",
            "domain": "example.invalid",
        },
    )


@pytest.fixture
def stub_places(monkeypatch):
    """Replace google_places.search_text with a deterministic enrichment payload."""
    def fake_search(brand, city, **kwargs):
        return [{
            "source": "google_places",
            "brand": brand,
            "place_id": f"{city.lower()[:3]}-1",
            "city": city,
            "title": f"{city} 1 (enriched)",
            "rating": 4.3,
            "review_count": 120,
            "phone": "+91-11-0000000",
            "latitude": 28.61 if city == "Delhi" else 19.07,
            "longitude": 77.2 if city == "Delhi" else 72.87,
        }]
    from src.fetchers import google_places
    monkeypatch.setattr(google_places, "search_text", fake_search)
    # Enable enrichment path by simulating a key
    from src.config import settings as config
    monkeypatch.setattr(config, "GOOGLE_PLACES_API_KEY", "test-key")


def test_stage1_national_scrape_populates_all_cities(temp_db, stub_scraper, stub_places):
    df, meta = cache_manager.smart_fetch_with_enrichment("Acme", ["Delhi"])
    assert meta["stage1_ran"] is True
    assert meta["stage1_records"] == 4
    # All 4 national stores now in DB
    assert db.count_enriched_stores_for_brand("Acme") == 4


def test_only_queried_cities_get_enriched(temp_db, stub_scraper, stub_places):
    df, meta = cache_manager.smart_fetch_with_enrichment("Acme", ["Delhi"])
    # stage2 enriched at most the Delhi store
    assert meta["stores_enriched_this_call"] >= 1
    # Mumbai and Bangalore stores exist but carry no enrichment stamp
    unenriched_mumbai = db.get_unenriched_store_ids("Acme", ["Mumbai"])
    unenriched_blr = db.get_unenriched_store_ids("Acme", ["Bangalore"])
    assert len(unenriched_mumbai) == 1
    assert len(unenriched_blr) == 1


def test_second_call_for_new_city_enriches_it(temp_db, stub_scraper, stub_places):
    # First call: Delhi
    cache_manager.smart_fetch_with_enrichment("Acme", ["Delhi"])
    # Second call: Mumbai. Stage1 should skip (cached full_scrape), stage2 enriches Mumbai.
    df, meta = cache_manager.smart_fetch_with_enrichment("Acme", ["Mumbai"])
    assert meta["stage1_ran"] is False
    unenriched_mumbai = db.get_unenriched_store_ids("Acme", ["Mumbai"])
    # Mumbai should be enriched now
    assert len(unenriched_mumbai) == 0


def test_fresh_enrichment_is_skipped(temp_db, stub_scraper, stub_places):
    # Seed a store with a recent enrichment stamp.
    sid = db.upsert_store({
        "brand": "Acme", "place_id": "del-1", "city": "Delhi", "title": "cached",
        "latitude": 28.61, "longitude": 77.2,
    })
    db.mark_store_enriched(sid)
    # get_unenriched_store_ids should not include this store for Delhi.
    stale = db.get_unenriched_store_ids("Acme", ["Delhi"])
    assert sid not in {r["store_id"] for r in stale}


def test_stale_enrichment_returns_to_queue(temp_db, stub_scraper):
    sid = db.upsert_store({
        "brand": "Acme", "place_id": "del-old", "city": "Delhi", "title": "old",
        "latitude": 28.61, "longitude": 77.2,
    })
    import time as _time
    # Put the enrichment stamp 10 days in the past.
    conn = db._get_conn()
    try:
        conn.execute(
            "UPDATE stores SET enriched_at = ?, enrichment_source = 'google_places' WHERE store_id = ?",
            (_time.time() - 10 * 86400, sid),
        )
        conn.commit()
    finally:
        conn.close()
    stale = db.get_unenriched_store_ids("Acme", ["Delhi"])
    assert sid in {r["store_id"] for r in stale}
