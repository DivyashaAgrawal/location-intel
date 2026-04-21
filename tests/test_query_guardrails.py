from __future__ import annotations

import pytest

from src.core import cache_manager, config, db


def test_small_brand_under_threshold(temp_db, no_api_keys):
    db.upsert_brand_metadata("SmallBrand", 30, "manual", 1.0)
    projected = cache_manager.estimate_enrichment_needed("SmallBrand", [])
    assert projected <= config.MAX_ENRICHMENT_CALLS_PER_QUERY


def test_large_brand_all_india_over_threshold(temp_db, no_api_keys):
    db.upsert_brand_metadata("HugeBrand", 500, "manual", 1.0)
    projected = cache_manager.estimate_enrichment_needed("HugeBrand", [])
    assert projected > config.MAX_ENRICHMENT_CALLS_PER_QUERY


def test_large_brand_single_city_under_threshold(temp_db, no_api_keys):
    db.upsert_brand_metadata("HugeBrand", 500, "manual", 1.0)
    projected = cache_manager.estimate_enrichment_needed("HugeBrand", ["Mumbai"])
    assert projected <= config.MAX_ENRICHMENT_CALLS_PER_QUERY


def test_enriched_stores_reduce_projection(temp_db, no_api_keys):
    db.upsert_brand_metadata("PartialBrand", 500, "manual", 1.0)
    # Seed 80 enriched Mumbai stores; that should reduce projection.
    for i in range(80):
        sid = db.upsert_store({
            "brand": "PartialBrand",
            "place_id": f"p{i}",
            "city": "Mumbai",
            "title": f"s{i}",
            "latitude": 19.0 + i * 0.001,
            "longitude": 72.8 + i * 0.001,
        })
        db.mark_store_enriched(sid, source="google_places")
    projected = cache_manager.estimate_enrichment_needed("PartialBrand", ["Mumbai"])
    # share = 1/20 * 500 = 25; already enriched (Mumbai) = 80 -> max(0, 25-80) = 0.
    assert projected == 0


def test_pipeline_blocks_large_queries(temp_db, no_api_keys, monkeypatch):
    db.upsert_brand_metadata("BlockBrand", 500, "manual", 1.0)

    import src.pipeline as pl

    monkeypatch.setattr(
        pl,
        "parse_query",
        lambda q: {
            "brands": ["BlockBrand"],
            "geography": {"level": "city", "filter": []},
            "comparison": False,
            "query_type": "brand",
            "category": None,
            "search_query": None,
        },
    )
    result = pl.run_pipeline("anything")
    assert result.get("status") == "blocked"
    scope = result["scope"]
    assert scope["brand"] == "BlockBrand"
    assert scope["total_brand_size"] == 500
    assert scope["threshold"] == config.MAX_ENRICHMENT_CALLS_PER_QUERY
    assert scope["projected_api_calls"] > scope["threshold"]
    assert scope["tier_1_cities_available"]


def test_pipeline_allows_single_city_query(temp_db, no_api_keys, monkeypatch):
    db.upsert_brand_metadata("BlockBrand", 500, "manual", 1.0)

    import src.pipeline as pl

    monkeypatch.setattr(
        pl,
        "parse_query",
        lambda q: {
            "brands": ["BlockBrand"],
            "geography": {"level": "city", "filter": ["Mumbai"]},
            "comparison": False,
            "query_type": "brand",
            "category": None,
            "search_query": None,
        },
    )
    result = pl.run_pipeline("BlockBrand Mumbai")
    assert result.get("status") != "blocked"


def test_blocked_response_includes_already_enriched_cities(temp_db, no_api_keys, monkeypatch):
    db.upsert_brand_metadata("AlreadyCovered", 500, "manual", 1.0)
    for i in range(3):
        sid = db.upsert_store({
            "brand": "AlreadyCovered",
            "place_id": f"p{i}",
            "city": "Delhi",
            "title": f"s{i}",
        })
        db.mark_store_enriched(sid)

    import src.pipeline as pl

    monkeypatch.setattr(
        pl,
        "parse_query",
        lambda q: {
            "brands": ["AlreadyCovered"],
            "geography": {"level": "city", "filter": []},
            "comparison": False,
            "query_type": "brand",
            "category": None,
            "search_query": None,
        },
    )
    result = pl.run_pipeline("anything")
    scope = result["scope"]
    cities = {c["city"] for c in scope["already_enriched_cities"]}
    assert "Delhi" in cities
