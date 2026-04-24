from __future__ import annotations

import logging
import time

from src.cache import db

logger = logging.getLogger(__name__)

BRAND_METADATA_TTL_SEC = 90 * 24 * 3600


def estimate_brand_size(brand: str, force_refresh: bool = False) -> dict:
    """
    Fast, cheap store-count estimate. Decision tree:
      1. Cache hit (<90 days old, not forced)   -> return cached.
      2. Brand has headline_count_selector      -> Playwright headline read.
      3. Brand in registry (no headline)        -> full scrape, count.
      4. Brand NOT in registry                  -> Places Text Search,
                                                   paginated up to 3 pages.
      5. All methods fail                       -> count=None, conf=0.
    """
    start = time.time()

    if not force_refresh:
        cached = db.get_brand_metadata(brand)
        if cached and cached.get("last_refreshed"):
            age = time.time() - float(cached["last_refreshed"])
            if age < BRAND_METADATA_TTL_SEC:
                return _shape_brand_size_result(brand, cached, is_stale=False, latency_ms=0)

    total: int | None = None
    source = "unknown"
    confidence = 0.0
    full_scrape_ts: float | None = None

    from src.fetchers import brand_scraper

    info = brand_scraper.get_brand_info(brand)
    if info is not None:
        if info.get("headline_count_selector"):
            try:
                total = brand_scraper.get_headline_count(brand)
            except Exception as e:
                logger.info("[brand_size] headline read failed for %s: %s", brand, e)
                total = None
            if total is not None:
                source = "scraper_headline"
                confidence = 0.95

        if total is None and info.get("extraction_method") in {"api", "html", "playwright"}:
            total, full_scrape_ts = _estimate_via_full_scrape(brand)
            if total is not None:
                source = "full_scrape"
                confidence = 1.0

    if total is None:
        total, hit_cap = _estimate_via_places_pagination(brand)
        if total is not None:
            source = "places_pagination_cap"
            confidence = 0.5 if hit_cap else 0.8

    latency_ms = int((time.time() - start) * 1000)

    if total is not None:
        db.upsert_brand_metadata(
            brand=brand,
            total_stores_estimate=total,
            source=source,
            confidence=confidence,
            full_scrape_completed_at=full_scrape_ts,
        )

    cached = db.get_brand_metadata(brand) or {
        "total_stores_estimate": total,
        "total_stores_source": source,
        "estimate_confidence": confidence,
        "last_refreshed": time.time(),
        "known_cities": [],
    }
    return _shape_brand_size_result(brand, cached, is_stale=False, latency_ms=latency_ms)


def _shape_brand_size_result(
    brand: str, cached: dict, is_stale: bool, latency_ms: int,
) -> dict:
    total = cached.get("total_stores_estimate")
    enriched = db.count_enriched_stores_for_brand(brand)
    coverage = None
    if total and total > 0:
        coverage = round(enriched / total * 100, 1)
    return {
        "brand": brand,
        "total_stores_estimate": total,
        "source": cached.get("total_stores_source"),
        "confidence": cached.get("estimate_confidence") or 0.0,
        "last_refreshed": cached.get("last_refreshed"),
        "known_cities": cached.get("known_cities", []),
        "coverage_pct": coverage,
        "is_stale": is_stale,
        "fetch_latency_ms": latency_ms,
    }


def _estimate_via_full_scrape(brand: str) -> tuple[int | None, float | None]:
    try:
        from src.fetchers import brand_scraper
        df = brand_scraper.scrape_brand_stores(brand, cities=[])
    except Exception as e:
        logger.info("[brand_size] full scrape failed for %s: %s", brand, e)
        return None, None
    if df is None or df.empty:
        return None, None
    return int(len(df)), time.time()


def _estimate_via_places_pagination(brand: str) -> tuple[int | None, bool]:
    """
    Fallback for brands absent from the scraper registry: paginate Google
    Places across a handful of top metros and use the union row count as a
    size estimate. hit_cap=True flags that the true count is >= returned
    value (Google caps at 60 rows via 3 pages of 20).
    """
    try:
        from src.fetchers import google_places
    except Exception:
        return None, False

    seen: set[str] = set()
    total_rows = 0
    hit_cap = False

    for city in ("Delhi", "Mumbai", "Bangalore"):
        try:
            records = google_places.search_text(brand, city, max_pages=3)
        except Exception as e:
            logger.info("[brand_size] places fallback error in %s: %s", city, e)
            records = []
        if not records:
            continue
        for r in records:
            pid = r.get("place_id") or f"{r.get('title')}|{r.get('address')}"
            if pid and pid not in seen:
                seen.add(pid)
                total_rows += 1
        if len(records) >= 60:
            hit_cap = True

    if total_rows == 0:
        return None, False
    return total_rows, hit_cap


def estimate_enrichment_needed(brand: str, cities: list[str]) -> int:
    """
    Project how many Google Places enrichment calls running the query now
    would cost. Subtracts stores already freshly enriched in the DB.

    Assumptions:
      - No cities (or "all India"): projection == full store count.
      - Otherwise: per-city share approximated as
        min(len(cities)/20, 1.0) of the national count.
      - Stores already enriched (and still fresh) don't count as projected.
    """
    size = estimate_brand_size(brand)
    total = size.get("total_stores_estimate") or 0

    is_all_india = (not cities) or any(
        c.strip().lower() in {"all india", "india"} for c in cities
    )
    if is_all_india:
        projected = total
    else:
        share = min(len(cities) / 20.0, 1.0) if cities else 1.0
        projected = int(round(total * share))

    already = db.count_enriched_stores_for_brand(brand, cities or None)
    return max(0, int(projected) - int(already))


def get_already_enriched_cities(brand: str) -> list[dict]:
    """Return [{city, store_count}] for cities with any enriched stores."""
    conn = db._get_conn()
    try:
        rows = conn.execute(
            """
            SELECT city, COUNT(*) AS n FROM stores
            WHERE brand = ? AND enriched_at IS NOT NULL AND city IS NOT NULL
            GROUP BY city
            ORDER BY n DESC
            """,
            (brand,),
        ).fetchall()
    finally:
        conn.close()
    return [{"city": r["city"], "store_count": int(r["n"])} for r in rows]
