"""
Playwright-based brand scraper for JS-rendered store locators.

Entry point: `scrape_with_playwright(brand, cities) -> pd.DataFrame`.

Playwright is imported lazily. If the package is not installed, this module
still imports cleanly and `PLAYWRIGHT_AVAILABLE == False`; callers fall back
to HTTP scrapers and the pipeline degrades gracefully.

Registry entries for Playwright-extractable brands carry the extra fields
`locator_url`, `wait_selector`, `item_selector`, `fields`, `load_more_selector`,
`max_clicks`, `headline_count_selector`, and `headline_count_regex` (the last
two consumed by Phase 1.5).
"""
from __future__ import annotations

import logging
import re
from typing import Any

import pandas as pd
from bs4 import BeautifulSoup

from src.fetchers._common import extract_pincode

logger = logging.getLogger(__name__)

try:
    from playwright.sync_api import TimeoutError as PWTimeoutError
    from playwright.sync_api import sync_playwright

    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    sync_playwright = None  # type: ignore[assignment]

    class PWTimeoutError(Exception):  # type: ignore[no-redef]
        pass


PAGE_TIMEOUT_MS = 30_000
HEADLINE_TIMEOUT_MS = 3_000
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)


def scrape_with_playwright(brand: str, cities: list[str]) -> pd.DataFrame:
    """Render the brand's store locator and extract stores.

    Returns records in the multi_fetcher schema with `source="brand_website"`
    and `confidence=0.95`. Returns an empty DataFrame if Playwright is
    unavailable, the registry lacks a Playwright config, the page times out,
    or no stores could be parsed.
    """
    if not PLAYWRIGHT_AVAILABLE:
        logger.info("  Playwright not installed; skipping JS scrape for %s", brand)
        return pd.DataFrame()

    from src.fetchers.brand_scraper import get_brand_info

    info = get_brand_info(brand)
    if not info or info.get("extraction_method") != "playwright":
        return pd.DataFrame()

    locator_url = info.get("locator_url") or info.get("store_locator_url")
    if not locator_url:
        logger.warning("  %s has no locator URL in registry; skipping", brand)
        return pd.DataFrame()

    wait_selector = info.get("wait_selector") or info.get("item_selector")
    item_selector = info.get("item_selector")
    fields = info.get("fields") or {}
    load_more_selector = info.get("load_more_selector")
    max_clicks = int(info.get("max_clicks") or 0)

    html = _render_page(locator_url, wait_selector, load_more_selector, max_clicks, brand)
    if not html:
        return pd.DataFrame()

    records = _parse_rendered_html(html, brand, item_selector, fields)
    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    df = _filter_by_cities(df, cities)
    return df


def get_headline_count(brand: str) -> int | None:
    """Read the brand's total store count from the locator page header.

    Uses a short (3 second) timeout. Returns None if Playwright is
    unavailable, the registry has no headline selector for this brand, or
    extraction fails. Consumed by Phase 1.5 brand-size estimation.
    """
    if not PLAYWRIGHT_AVAILABLE:
        return None

    from src.fetchers.brand_scraper import get_brand_info

    info = get_brand_info(brand)
    if not info:
        return None

    selector = info.get("headline_count_selector")
    locator_url = info.get("locator_url") or info.get("store_locator_url")
    if not selector or not locator_url:
        return None

    regex = info.get("headline_count_regex")

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            context = browser.new_context(user_agent=USER_AGENT)
            page = context.new_page()
            page.set_default_timeout(HEADLINE_TIMEOUT_MS)
            try:
                page.goto(locator_url, wait_until="domcontentloaded")
                page.wait_for_selector(selector, timeout=HEADLINE_TIMEOUT_MS)
                text = page.locator(selector).first.text_content() or ""
            finally:
                context.close()
                browser.close()
    except Exception as e:
        logger.info("  [%s] headline count extraction failed: %s", brand, e)
        return None

    return _parse_count(text, regex)


def _render_page(
    url: str,
    wait_selector: str | None,
    load_more_selector: str | None,
    max_clicks: int,
    brand: str,
) -> str | None:
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            context = browser.new_context(user_agent=USER_AGENT)
            page = context.new_page()
            page.set_default_timeout(PAGE_TIMEOUT_MS)
            try:
                page.goto(url, wait_until="domcontentloaded")
                if wait_selector:
                    try:
                        page.wait_for_selector(wait_selector, timeout=PAGE_TIMEOUT_MS)
                    except PWTimeoutError:
                        logger.info(
                            "  [%s] wait_selector %r did not appear within timeout",
                            brand, wait_selector,
                        )
                        return None

                if load_more_selector and max_clicks > 0:
                    _exhaust_load_more(page, load_more_selector, max_clicks)

                return page.content()
            finally:
                context.close()
                browser.close()
    except Exception as e:
        logger.warning("  [%s] Playwright render failed: %s", brand, e)
        return None


def _exhaust_load_more(page: Any, selector: str, max_clicks: int) -> None:
    for _ in range(max_clicks):
        try:
            locator = page.locator(selector)
            if locator.count() == 0:
                return
            first = locator.first
            if not first.is_visible():
                return
            first.click()
            page.wait_for_timeout(500)
        except Exception:
            return


def _parse_rendered_html(
    html: str,
    brand: str,
    item_selector: str | None,
    fields: dict,
) -> list[dict]:
    if not item_selector:
        return []

    soup = BeautifulSoup(html, "html.parser")
    items = soup.select(item_selector)
    records: list[dict] = []

    for item in items:
        rec: dict[str, Any] = {
            "source": "brand_website",
            "brand": brand,
            "confidence": 0.95,
        }
        for key, sel in fields.items():
            rec[key] = _first_text(item, sel)

        address = str(rec.get("address") or "")
        if not rec.get("pincode") and address:
            rec["pincode"] = extract_pincode(address)

        if rec.get("title") or rec.get("address"):
            records.append(rec)

    return records


def _first_text(scope: Any, selector: str) -> str | None:
    if not selector:
        return None
    el = scope.select_one(selector)
    if el is None:
        return None
    txt = el.get_text(separator=" ", strip=True)
    return txt or None


def _filter_by_cities(df: pd.DataFrame, cities: list[str]) -> pd.DataFrame:
    if df.empty or not cities:
        return df
    lowered = {c.strip().lower() for c in cities if c}
    if "city" in df.columns and df["city"].notna().any():
        mask = df["city"].fillna("").str.lower().isin(lowered)
        if mask.any():
            return df[mask].reset_index(drop=True)
    if "address" in df.columns:
        pattern = "|".join(re.escape(c) for c in cities if c)
        if pattern:
            mask = df["address"].fillna("").str.contains(pattern, case=False, na=False)
            if mask.any():
                return df[mask].reset_index(drop=True)
    return df


def _parse_count(text: str, regex: str | None) -> int | None:
    if regex:
        m = re.search(regex, text)
        if not m:
            return None
        text = m.group(1) if m.groups() else m.group(0)
    digits = "".join(c for c in text if c.isdigit())
    return int(digits) if digits else None
