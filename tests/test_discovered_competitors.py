from __future__ import annotations

from src.analysis import competitor
from src.core import db
from src.fetchers import multi_fetcher


def test_record_is_idempotent_and_bumps_times_seen(temp_db):
    db.record_discovered_competitor("Joe's Pizza", "pizza")
    db.record_discovered_competitor("Joe's Pizza", "pizza")
    db.record_discovered_competitor("Joe's Pizza", "pizza")
    rows = db.get_discovered_competitors("pizza")
    match = next(r for r in rows if r["brand"] == "Joe's Pizza")
    assert match["times_seen"] == 3


def test_get_competitors_merges_static_and_discovered(temp_db):
    db.record_discovered_competitor("Pizza Unicorn", "pizza")
    merged = competitor.get_competitors("Dominos Pizza", max_n=10)
    assert "Pizza Unicorn" in merged
    # Static competitors still present and first
    assert merged[0] == "Pizza Hut"


def test_verified_flag_prioritises_over_unverified(temp_db):
    db.record_discovered_competitor("NoiseBrand", "pizza")
    db.record_discovered_competitor("RealBrand", "pizza")
    db.verify_discovered_competitor("RealBrand")
    rows = db.get_discovered_competitors("pizza")
    assert rows[0]["brand"] == "RealBrand"
    assert rows[0]["manually_verified"] == 1


def test_delete_removes_row(temp_db):
    db.record_discovered_competitor("RemoveMe", "pizza")
    assert any(r["brand"] == "RemoveMe" for r in db.get_discovered_competitors("pizza"))
    db.delete_discovered_competitor("RemoveMe")
    assert not any(r["brand"] == "RemoveMe" for r in db.get_discovered_competitors("pizza"))


def test_multi_fetcher_records_only_unknown_brands(temp_db):
    records = [
        {"brand": "Dominos Pizza", "source": "google_places"},
        {"brand": "Pizza Cavalry", "source": "google_places"},
        {"brand": "", "source": "google_places"},
    ]
    multi_fetcher._record_discovered_brands_for_category(records, "pizza")
    names = {r["brand"] for r in db.get_discovered_competitors("pizza")}
    assert "Dominos Pizza" not in names  # already in BRAND_CATEGORY
    assert "Pizza Cavalry" in names
    assert "" not in names


def test_run_competitor_analysis_flags_tentative(temp_db):
    import pandas as pd
    # A brand-new low-confidence competitor
    db.record_discovered_competitor("Fresh Pizza Co", "pizza")
    focal_df = pd.DataFrame([
        {"brand": "Dominos Pizza", "city": "Delhi", "pincode": "110001"},
    ])
    result = competitor.run_competitor_analysis(
        focal_brand="Dominos Pizza",
        focal_stores=focal_df,
        cities=["Delhi"],
        fetch_fn=lambda b, c: (pd.DataFrame(), "mock"),
        max_competitors=10,
    )
    tentative = result.get("tentative_competitors") or []
    if "Fresh Pizza Co" in result["competitors"]:
        assert "Fresh Pizza Co" in tentative
