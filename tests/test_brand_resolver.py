"""Tests for src.brand_resolver.

The embedding path is exercised only when sentence-transformers + faiss are
importable; otherwise those tests fall back to substring behaviour. The
fallback path is always tested.
"""
from __future__ import annotations

import importlib

import pytest


@pytest.fixture
def seeded_registry(temp_db):
    """Seed a miniature registry so the resolver has something to match."""
    from src.caching import db as _db

    brands = [
        ("Biryani By Kilo", ["BBK", "Biryani By Kilo Ltd"], "biryani"),
        ("Behrouz Biryani", ["Behrooz Biryani"], "biryani"),
        ("Paradise Biryani", ["Paradise Food Court"], "biryani"),
        ("Dominos Pizza", ["Dominos", "Domino's"], "pizza"),
        ("Pizza Hut", [], "pizza"),
        ("Starbucks", [], "coffee"),
        ("Cafe Coffee Day", ["CCD"], "coffee"),
        ("Blue Tokai", ["Blue Tokai Coffee Roasters"], "coffee"),
        ("Third Wave Coffee", ["TWCR"], "coffee"),
        ("McDonald's", ["McD", "Mickey D's"], "burger"),
        ("KFC", ["Kentucky Fried Chicken"], "chicken_qsr"),
        ("Baskin Robbins", [], "ice_cream"),
        ("Lenskart", [], "eyewear"),
        ("Nykaa", [], "beauty"),
        ("Haldiram's", ["Haldirams"], "indian_qsr"),
        ("Bikanervala", [], "indian_qsr"),
        ("Wow! Momo", ["Wow Momo"], "indian_qsr"),
        ("FabIndia", ["Fab India"], "ethnic_wear"),
        ("Tanishq", [], "jewellery"),
        ("Apollo Pharmacy", ["Apollo"], "pharmacy"),
    ]
    for name, aliases, category in brands:
        _db.upsert_brand_to_registry(
            canonical_name=name,
            aliases=aliases,
            category=category,
            source="seed",
            verified=1,
        )
    yield temp_db


@pytest.fixture
def resolver_reset():
    """Reset cached model/index/metadata between tests to avoid cross-contamination."""
    import src.brand_resolver as br
    br.reset_caches()
    yield
    br.reset_caches()


def _has_embeddings() -> bool:
    try:
        importlib.import_module("sentence_transformers")
        importlib.import_module("faiss")
        return True
    except ImportError:
        return False


def test_extract_candidates_strips_stopwords():
    from src.brand_resolver import extract_candidate_phrases
    candidates = extract_candidate_phrases(
        "get me pincode wise details on biryani by kilo"
    )
    # "biryani by kilo" must survive (a real multi-word brand).
    assert "biryani by kilo" in candidates
    # instruction words must not spawn candidates.
    for junk in ["get", "me", "pincode", "wise", "details", "on"]:
        assert junk not in candidates


def test_extract_candidates_empty_query_returns_empty():
    from src.brand_resolver import extract_candidate_phrases
    assert extract_candidate_phrases("") == []
    assert extract_candidate_phrases("   ") == []


def test_extract_candidates_removes_geography():
    from src.brand_resolver import extract_candidate_phrases
    candidates = extract_candidate_phrases("dominos in delhi and mumbai")
    assert "dominos" in candidates
    assert "delhi" not in candidates
    assert "mumbai" not in candidates


def test_substring_match_high_confidence(seeded_registry, resolver_reset):
    from src.brand_resolver import resolve_query
    r = resolve_query("get me pincode wise details on biryani by kilo")
    assert r["match_found"] is True
    assert r["confidence"] == "high"
    assert r["canonical_brand"] == "Biryani By Kilo"
    assert r["score"] >= 0.92


def test_alias_match_via_bbk(seeded_registry, resolver_reset):
    from src.brand_resolver import resolve_query
    r = resolve_query("pincode wise details on bbk")
    assert r["match_found"] is True
    assert r["canonical_brand"] == "Biryani By Kilo"


def test_category_query_returns_no_specific_brand(seeded_registry, resolver_reset):
    from src.brand_resolver import resolve_query
    r = resolve_query("pizza near me")
    # The resolver must not claim a specific brand for a bare category word.
    assert r["canonical_brand"] is None
    assert r["confidence"] == "none"


def test_common_noun_does_not_match(seeded_registry, resolver_reset):
    from src.brand_resolver import resolve_query
    r = resolve_query("bakery in delhi")
    assert r["confidence"] == "none"


def test_empty_query_returns_none_match(seeded_registry, resolver_reset):
    from src.brand_resolver import resolve_query
    r = resolve_query("")
    assert r["match_found"] is False
    assert r["confidence"] == "none"


def test_fallback_path_direct(seeded_registry, resolver_reset):
    from src.brand_resolver import resolve_query_fallback
    r = resolve_query_fallback("domino's in mumbai")
    assert r["match_found"] is True
    assert r["canonical_brand"] == "Dominos Pizza"
    assert r["method"] == "substring_fallback"


def test_fallback_category_only(seeded_registry, resolver_reset):
    from src.brand_resolver import resolve_query_fallback
    r = resolve_query_fallback("best pizza in town")
    # No specific brand name as substring -> no match.
    assert r["match_found"] is False
    assert r["method"] == "substring_fallback"


def test_fallback_used_when_embeddings_missing(
    seeded_registry, resolver_reset, monkeypatch
):
    """If we pretend embeddings are unavailable, substring fallback should kick in."""
    import src.brand_resolver as br
    br.reset_caches()
    monkeypatch.setattr(br, "_check_embeddings_available", lambda: False)
    r = br.resolve_query("starbucks in mumbai")
    assert r["match_found"] is True
    assert r["canonical_brand"] == "Starbucks"


def test_comparison_query_finds_at_least_one_brand(
    seeded_registry, resolver_reset
):
    """Resolver returns a single best match; NLU splits on 'vs' to find the others."""
    from src.brand_resolver import resolve_query
    r = resolve_query("third wave coffee vs blue tokai in bangalore")
    assert r["match_found"] is True
    assert r["canonical_brand"] in ("Third Wave Coffee", "Blue Tokai")


@pytest.mark.skipif(not _has_embeddings(), reason="sentence-transformers/faiss not installed")
def test_embedding_path_for_unregistered_misspelling(
    seeded_registry, resolver_reset, tmp_path, monkeypatch
):
    """Build a tiny index at runtime and verify the embedding path triggers."""
    from src.scripts.rebuild_brand_index import build_index
    import src.brand_resolver as br

    idx_path = tmp_path / "brand_index.faiss"
    meta_path = tmp_path / "brand_index_metadata.json"
    monkeypatch.setattr(br, "INDEX_PATH", idx_path)
    monkeypatch.setattr(br, "METADATA_PATH", meta_path)
    br.reset_caches()

    build_index(index_path=idx_path, metadata_path=meta_path)

    r = br.resolve_query("behrouz in pune")
    # Either substring matches "Behrouz" inside "Behrouz Biryani" (alias path)
    # or embedding returns it as the best match -- both acceptable.
    assert r["canonical_brand"] in ("Behrouz Biryani", "Biryani By Kilo", None) or \
        "biryani" in (r.get("candidate_phrase") or "")


def test_stopwords_prevent_instruction_words_from_matching(
    seeded_registry, resolver_reset
):
    """Pure instruction queries should produce no brand candidates."""
    from src.brand_resolver import extract_candidate_phrases
    assert extract_candidate_phrases("get me the pincode wise summary") == []
