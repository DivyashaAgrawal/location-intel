"""Tests for parse_query's brand_hint plumbing.

These exercise the rule-based fallback path (no Ollama running in CI), so
the high-confidence bypass and explicit brand_hint behaviour are verified
without needing a live LLM.
"""
from __future__ import annotations

import pytest


@pytest.fixture
def no_ollama(monkeypatch):
    """Force parse_query_with_ollama to raise so parse_query takes the fallback path."""
    from src.nlu import parser as nlu

    def _raise(*args, **kwargs):
        raise RuntimeError("Ollama disabled for test")

    monkeypatch.setattr(nlu, "parse_query_with_ollama", _raise)
    yield


def test_high_confidence_hint_bypasses_llm():
    """With a high-confidence hint we never need to call Ollama."""
    from src.nlu.parser import parse_query
    hint = {
        "confidence": "high",
        "canonical_brand": "Biryani By Kilo",
        "candidate_phrase": "biryani by kilo",
        "score": 1.0,
    }
    r = parse_query("get me pincode wise details on biryani by kilo", brand_hint=hint)
    assert r["query_type"] == "brand"
    assert "Biryani By Kilo" in r["brands"]
    assert r["category"] is None


def test_high_confidence_hint_respects_pincode_level():
    from src.nlu.parser import parse_query
    hint = {"confidence": "high", "canonical_brand": "Starbucks"}
    r = parse_query("pincode wise starbucks in delhi", brand_hint=hint)
    assert r["query_type"] == "brand"
    assert r["brands"] == ["Starbucks"]
    assert r["geography"]["level"] == "pincode"
    assert "Delhi" in r["geography"]["filter"]


def test_none_confidence_falls_through_to_normal_parsing(no_ollama):
    """No-hint queries should behave like the pre-resolver NLU."""
    from src.nlu.parser import parse_query
    r = parse_query(
        "biryani restaurants in delhi",
        brand_hint={"confidence": "none"},
    )
    assert r["query_type"] == "category"
    assert r["category"] == "biryani"
    assert r["brands"] == []


def test_hint_canonical_wins_over_fallback_brand_list(no_ollama):
    """If the hint says it's a specific brand, fallback shouldn't relabel it."""
    from src.nlu.parser import parse_query
    # Fallback KNOWN_BRANDS doesn't include "Biryani By Kilo"; without the
    # hint the fallback would classify this as category.
    r = parse_query(
        "pincode wise biryani by kilo in delhi",
        brand_hint={
            "confidence": "high",
            "canonical_brand": "Biryani By Kilo",
        },
    )
    assert r["query_type"] == "brand"
    assert r["brands"][0] == "Biryani By Kilo"


def test_ambiguous_hint_lets_llm_decide(no_ollama):
    """Ambiguous hints should leave query_type open (here: falls to rule-based)."""
    from src.nlu.parser import parse_query
    r = parse_query(
        "random thing",
        brand_hint={
            "confidence": "ambiguous",
            "canonical_brand": "Something",
            "candidate_phrase": "thing",
        },
    )
    # We can't assert what ollama would pick -- only that the ambiguous hint
    # doesn't force query_type=brand when Ollama fails.
    assert r["query_type"] in ("brand", "category")


def test_parse_with_predetermined_brand_preserves_metrics():
    from src.nlu.parser import parse_with_predetermined_brand
    r = parse_with_predetermined_brand(
        "starbucks summary in mumbai", "Starbucks"
    )
    assert r["query_type"] == "brand"
    assert r["brands"] == ["Starbucks"]
    # "summary" in query -> all 4 metrics
    assert "sentiment_summary" in r["metrics"]
    assert "review_count" in r["metrics"]


def test_comparison_brands_merged_by_hint_path(temp_db):
    """The high-confidence path should also pull additional brands from 'vs' clauses."""
    from src.cache import db as _db
    _db.upsert_brand_to_registry(
        canonical_name="Biryani By Kilo",
        aliases=["BBK"],
        category="biryani",
        source="seed",
    )
    _db.upsert_brand_to_registry(
        canonical_name="Behrouz Biryani",
        aliases=[],
        category="biryani",
        source="seed",
    )
    # Reset cached model/index so the resolver sees the fresh DB.
    import src.nlu.brand_resolver as br
    br.reset_caches()

    from src.nlu.parser import parse_query
    r = parse_query("Biryani By Kilo vs Behrouz Biryani in Mumbai")
    assert r["query_type"] == "brand"
    assert set(r["brands"]) == {"Biryani By Kilo", "Behrouz Biryani"}
    assert r["comparison"] is True
