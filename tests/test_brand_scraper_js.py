from __future__ import annotations

import pandas as pd

from src.fetchers import brand_scraper, brand_scraper_js


def test_module_imports_without_playwright(monkeypatch):
    """Module must import cleanly and expose PLAYWRIGHT_AVAILABLE."""
    assert hasattr(brand_scraper_js, "PLAYWRIGHT_AVAILABLE")
    assert isinstance(brand_scraper_js.PLAYWRIGHT_AVAILABLE, bool)


def test_scrape_with_playwright_returns_empty_when_unavailable(monkeypatch):
    monkeypatch.setattr(brand_scraper_js, "PLAYWRIGHT_AVAILABLE", False)
    df = brand_scraper_js.scrape_with_playwright("Starbucks", ["Delhi"])
    assert df.empty


def test_scrape_with_playwright_empty_for_unknown_brand(monkeypatch):
    monkeypatch.setattr(brand_scraper_js, "PLAYWRIGHT_AVAILABLE", True)
    df = brand_scraper_js.scrape_with_playwright("UnknownBrandXYZ", ["Delhi"])
    assert df.empty


def test_parse_rendered_html_extracts_records():
    html = """
    <html><body>
      <div class="store-result-item">
        <span class="store-name">Connaught Place</span>
        <span class="store-address">12 Barakhamba Road, Delhi 110001</span>
        <span class="store-phone">011-12345678</span>
      </div>
      <div class="store-result-item">
        <span class="store-name">Saket</span>
        <span class="store-address">Saket District Centre, Delhi 110017</span>
      </div>
    </body></html>
    """
    fields = {
        "title": ".store-name",
        "address": ".store-address",
        "phone": ".store-phone",
    }
    records = brand_scraper_js._parse_rendered_html(
        html, "Starbucks", ".store-result-item", fields
    )
    assert len(records) == 2
    assert records[0]["title"] == "Connaught Place"
    assert records[0]["pincode"] == "110001"
    assert records[0]["confidence"] == 0.95
    assert records[1]["phone"] is None  # second store has no phone element


def test_parse_count_plain_number_and_regex():
    assert brand_scraper_js._parse_count("954 stores", None) == 954
    assert brand_scraper_js._parse_count("We have 954 stores", r"(\d+)\s*stores?") == 954
    assert brand_scraper_js._parse_count("no number here", None) is None


def test_dispatcher_handles_playwright_method_without_crash(monkeypatch):
    """scrape_brand_stores for a playwright brand returns empty DF if Playwright missing."""
    monkeypatch.setattr(brand_scraper_js, "PLAYWRIGHT_AVAILABLE", False)
    df = brand_scraper.scrape_brand_stores("Starbucks", ["Delhi"])
    assert isinstance(df, pd.DataFrame)
    assert df.empty


def test_dispatcher_remaps_js_rendered_to_playwright(monkeypatch, caplog):
    """'js_rendered' must auto-remap; no crash."""
    monkeypatch.setattr(brand_scraper_js, "PLAYWRIGHT_AVAILABLE", False)
    monkeypatch.setitem(
        brand_scraper.BRAND_REGISTRY["Starbucks"], "extraction_method", "js_rendered"
    )
    df = brand_scraper.scrape_brand_stores("Starbucks", ["Delhi"])
    assert df.empty
