"""
Multi-source data fetcher (orchestrator only).

Each external data source lives in its own module inside
`src/fetchers/`. This file wires them together: picks which
sources to hit, enforces the non-negotiable "Serper only if Google Places
was empty" rule, memos results in the low-level blob cache, and returns
one combined DataFrame for downstream reconciliation.

Each adapter returns `list[dict]` in a common schema:
    source, brand, title, address, city, state, pincode,
    latitude, longitude, rating, review_count, phone, website,
    category, reviews_text, confidence.
"""
from __future__ import annotations

import logging
import time

import pandas as pd

from src.cache import manager as cache_manager
from src.fetchers import brand_scraper, google_places, osm, serper
from src.fetchers._common import extract_pincode

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Adapter wrappers -- thin shims that normalise each adapter's entry point.
# ---------------------------------------------------------------------------

def _fetch_google_places(query: str, city: str) -> list[dict]:
    return google_places.search_text(query, city)


def _fetch_serper(query: str, city: str) -> list[dict]:
    return serper.fetch(query, city)


def _fetch_osm(query: str, city: str) -> list[dict]:
    return osm.fetch(query, city)


def _fetch_outscraper(query: str, city: str) -> list[dict]:
    """Stub until Outscraper is configured (see docs/architecture.md)."""
    return []


def _fetch_brand_website(query: str, city: str) -> list[dict]:
    """
    Brand-website adapter. Looks up `query` in the brand scraper registry;
    returns [] for unknown / blocked / JS-rendered brands so the reconciler
    silently relies on other sources.
    """
    info = brand_scraper.get_brand_info(query)
    if info is None:
        return []
    # `playwright`-method brands run via scrape_brand_stores -> brand_scraper_js.
    # `blocked` brands short-circuit so reconciliation falls through to Google/Serper/OSM.
    if info.get("extraction_method") == "blocked":
        return []

    try:
        df = brand_scraper.scrape_brand_stores(query, [city])
    except Exception as e:
        logger.warning(f"[brand_website] scrape error for '{query}' in {city}: {e}")
        return []

    if df.empty:
        return []

    records = []
    for row in df.to_dict(orient="records"):
        records.append({
            "source": "brand_website",
            "brand": row.get("brand") or query,
            "title": row.get("title", ""),
            "address": row.get("address", ""),
            "city": row.get("city", city),
            "state": row.get("state"),
            "pincode": row.get("pincode") or extract_pincode(str(row.get("address", ""))),
            "latitude": row.get("latitude"),
            "longitude": row.get("longitude"),
            "rating": None,
            "review_count": None,
            "phone": row.get("phone"),
            "website": row.get("website"),
            "category": row.get("category"),
            "reviews_text": None,
            "confidence": 0.8,
        })
    return records


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def _record_discovered_brands_for_category(records: list[dict], category: str) -> None:
    """For category queries, persist any novel brand names to `discovered_competitors`."""
    if not records or not category:
        return
    try:
        from src.analysis.competitor import BRAND_CATEGORY
        from src.cache import db as _db
    except Exception:
        return

    known = {b.lower() for b in BRAND_CATEGORY}
    seen_this_call: set[str] = set()
    for rec in records:
        name = (rec.get("brand") or rec.get("title") or "").strip()
        if not name:
            continue
        lower = name.lower()
        if lower in known or lower in seen_this_call:
            continue
        seen_this_call.add(lower)
        try:
            _db.record_discovered_competitor(name, category=category, source=rec.get("source") or "category_query")
        except Exception:
            pass


def fetch_multi_source(
    query: str,
    cities: list[str],
    sources: list[str] | None = None,
    delay: float = 1.0,
    category: str | None = None,
) -> pd.DataFrame:
    """
    Run the requested adapters against each city and return a combined
    DataFrame. The `source` column identifies which adapter produced each row.

    Default source order:
        brand_website -> google_places -> serper (fallback) -> osm
    """
    if sources is None:
        sources = ["brand_website", "google_places", "serper", "osm"]

    source_adapters = {
        "google_places": _fetch_google_places,
        "serper":        _fetch_serper,
        "osm":           _fetch_osm,
        "outscraper":    _fetch_outscraper,
        "brand_website": _fetch_brand_website,
    }

    all_records: list[dict] = []

    for city in cities:
        google_places_had_results = False

        for source_name in sources:
            adapter = source_adapters.get(source_name)
            if not adapter:
                continue

            # Non-negotiable rule: Serper only runs if Google Places came
            # back empty for this city in this run.
            if (
                source_name == "serper"
                and "google_places" in sources
                and google_places_had_results
            ):
                logger.info(
                    f"[serper] skipped for '{query}' in {city} "
                    "(google_places returned results)"
                )
                continue

            cached_df = cache_manager.get_cached(query, city, source=source_name)
            if cached_df is not None and not cached_df.empty:
                all_records.extend(cached_df.to_dict(orient="records"))
                if source_name == "google_places":
                    google_places_had_results = True
                logger.info(
                    f"[{source_name}] cache hit: {len(cached_df)} results "
                    f"for '{query}' in {city}"
                )
                continue

            try:
                records = adapter(query, city)
                all_records.extend(records)
                if source_name == "google_places" and records:
                    google_places_had_results = True
                logger.info(
                    f"[{source_name}] {len(records)} results for '{query}' in {city}"
                )
                if records:
                    cache_manager.set_cached(
                        query, city, pd.DataFrame(records), source=source_name
                    )
                    if category:
                        _record_discovered_brands_for_category(records, category)
            except Exception as e:
                logger.warning(f"[{source_name}] Error for '{query}' in {city}: {e}")

            if delay > 0:
                time.sleep(delay)

    df = pd.DataFrame(all_records)

    if not df.empty and "latitude" in df.columns:
        df = df.drop_duplicates(subset=["source", "latitude", "longitude"], keep="first")

    return df
