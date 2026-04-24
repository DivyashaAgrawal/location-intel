"""
Unified cache facade.

Two public surfaces:

1. Low-level blob cache (`get_cached` / `set_cached`)
   Redis-first, SQLite-fallback via `db.source_cache`. Keyed by
   `(brand, city, source)`. Used by multi_fetcher to memo each adapter's
   raw records.

2. High-level query orchestrator (`smart_fetch(brand, city) -> (df, source)`)
   The three-layer stack the IC-prep workflow runs on:
       Redis -> persistent DB (query_cache) -> live API -> mock (last resort)
   Returns the unified-store DataFrame plus a label identifying which layer
   answered. Writes results back through DB + Redis so repeats are free.
"""
from __future__ import annotations

import hashlib
import json
import logging
import time
from collections.abc import Callable

import pandas as pd

from src.cache import db, redis_cache
from src.config import settings

logger = logging.getLogger(__name__)

_redis_down_until: float = 0.0

BRAND_METADATA_TTL_SEC = 90 * 24 * 3600


def _redis_ok() -> bool:
    return time.time() > _redis_down_until


def _mark_redis_down(err: Exception) -> None:
    global _redis_down_until
    if _redis_down_until < time.time():
        logger.warning("Redis unavailable (%s); will retry in 60s.", err)
    _redis_down_until = time.time() + 60.0


def get_cached(
    brand: str,
    city: str,
    source: str = "",
) -> pd.DataFrame | None:
    """Look up a cached DataFrame. Redis first, DB source_cache fallback."""
    if _redis_ok():
        try:
            result = redis_cache.get_cached(brand, city, source)
            if result is not None:
                return result
        except Exception as e:
            _mark_redis_down(e)
    return db.get_source_cache(brand, city, source, ttl=redis_cache.ttl_for(source))


def set_cached(
    brand: str,
    city: str,
    df: pd.DataFrame,
    source: str = "",
    ttl: int | None = None,
) -> None:
    """
    Write to Redis (if available) and always to the DB source_cache.

    If `ttl` is None, `redis_cache.ttl_for(source)` is used so each source
    gets its own freshness policy (brand_website = 7d, serper = 1d, osm = 30d).
    The DB write records `fetched_at`; the TTL is applied on read.
    """
    if _redis_ok():
        try:
            redis_cache.set_cached(brand, city, source, df, ttl=ttl)
        except Exception as e:
            _mark_redis_down(e)
    try:
        db.set_source_cache(brand, city, source, df)
    except Exception as e:
        logger.warning("source_cache write failed: %s", e)


# ---------------------------------------------------------------------------
# smart_fetch: three-layer orchestrator for a (brand, city) query
# ---------------------------------------------------------------------------

QUERY_REDIS_PREFIX = "loc_intel:query:"


def _query_redis_key(brand: str, city: str) -> str:
    key = f"{brand.strip().lower()}|{city.strip().lower()}"
    return QUERY_REDIS_PREFIX + hashlib.sha1(key.encode("utf-8")).hexdigest()


def _redis_client():
    if not _redis_ok():
        return None
    try:
        return redis_cache._get_client()
    except Exception as e:
        _mark_redis_down(e)
        return None


def _redis_get_query(brand: str, city: str) -> pd.DataFrame | None:
    client = _redis_client()
    if client is None:
        return None
    try:
        payload = client.get(_query_redis_key(brand, city))
        if payload is None:
            return None
        records = json.loads(payload)
    except Exception as e:
        _mark_redis_down(e)
        return None
    return pd.DataFrame(records) if records else pd.DataFrame()


def _redis_set_query(brand: str, city: str, df: pd.DataFrame, ttl: int = 24 * 3600) -> None:
    client = _redis_client()
    if client is None:
        return
    try:
        payload = df.to_json(orient="records", date_format="iso") if not df.empty else "[]"
        client.setex(_query_redis_key(brand, city), ttl, payload)
    except Exception as e:
        _mark_redis_down(e)


def smart_fetch(
    brand: str,
    city: str,
    *,
    live_fetcher: Callable[[str, str], pd.DataFrame] | None = None,
    reconciler: Callable[[pd.DataFrame], pd.DataFrame] | None = None,
    mock_fetcher: Callable[[str, str], pd.DataFrame] | None = None,
    max_db_age: int = db.QUERY_TTL_DEFAULT,
) -> tuple[pd.DataFrame, str]:
    """
    Three-layer fetch for a `(brand, city)` query.

    Order: Redis hot cache -> persistent DB (`query_cache`) -> live APIs ->
    last-resort mock. Returns `(df, source_label)` where `source_label`
    identifies which layer answered: one of `"redis"`, `"db"`, `"api"`,
    `"mock"`.

    `live_fetcher`, `reconciler`, and `mock_fetcher` are injected so this
    function is trivial to unit-test without the pipeline. Defaults wire up
    the real multi_fetcher + reconciler + a tiny mock fallback.
    """
    redis_df = _redis_get_query(brand, city)
    if redis_df is not None and not redis_df.empty:
        return redis_df, "redis"

    try:
        db_df = db.lookup_query(brand, city, max_age=max_db_age)
    except Exception as e:
        logger.warning("DB query_cache lookup failed: %s", e)
        db_df = None
    if db_df is not None and not db_df.empty:
        _redis_set_query(brand, city, db_df)
        return db_df, "db"

    if live_fetcher is None or reconciler is None:
        from src.fetchers import multi_fetcher
        from src.reconciler import reconciler as _rec

        live_fetcher = live_fetcher or (lambda b, c: multi_fetcher.fetch_multi_source(b, [c]))
        reconciler = reconciler or _rec.reconcile

    try:
        raw = live_fetcher(brand, city)
    except Exception as e:
        logger.warning("live fetch failed for %s / %s: %s", brand, city, e)
        raw = pd.DataFrame()

    if raw is not None and not raw.empty:
        try:
            merged = reconciler(raw)
        except Exception as e:
            logger.warning("reconcile failed for %s / %s: %s", brand, city, e)
            merged = raw

        if not merged.empty:
            store_ids = []
            for rec in merged.to_dict(orient="records"):
                rec.setdefault("brand", brand)
                rec.setdefault("city", city)
                store_ids.append(db.upsert_store(rec))
            db.save_query_result(brand, city, store_ids, source="api")
            _redis_set_query(brand, city, merged)
            return merged, "api"

    if mock_fetcher is None:
        mock_fetcher = _default_mock
    mock_df = mock_fetcher(brand, city)
    db.log_api_call("mock", brand=brand, city=city, success=True, cost=0.0)
    return mock_df, "mock"


def _default_mock(brand: str, city: str) -> pd.DataFrame:
    """
    Tiny deterministic placeholder used only when no API keys, no DB, and
    no Redis data are available. Keeps the demo path alive but visibly
    fake (one row, obviously synthetic address). Not suitable for analysis.
    """
    return pd.DataFrame([{
        "source": "mock",
        "brand": brand,
        "title": f"{brand} (MOCK)",
        "address": f"MOCK data - no API configured for {city}",
        "city": city,
        "state": None,
        "pincode": None,
        "latitude": None,
        "longitude": None,
        "rating": None,
        "review_count": None,
        "phone": None,
        "website": None,
        "category": None,
        "reviews_text": None,
        "confidence": 0.0,
        "sources": "mock",
        "source_count": 1,
        "data_quality": 0,
    }])


# ---------------------------------------------------------------------------
# Two-stage national scrape + per-city enrichment (Phase 2)
# ---------------------------------------------------------------------------

def smart_fetch_with_enrichment(
    brand: str,
    cities: list[str],
    query_type: str = "brand",
) -> tuple[pd.DataFrame, dict]:
    """
    Two-stage fetch: authoritative national footprint, then lazy per-city
    enrichment.

    Stage 1: if the brand is in the scraper registry, run the Playwright/HTTP
    scrape once (nationally), writing raw stores to the DB. Cached in
    `brand_metadata.full_scrape_completed_at` so repeat queries skip it.

    Stage 2: for stores in `cities` that lack a fresh enrichment stamp,
    call Google Places to add rating/phone/review_count. Stores in other
    cities stay un-enriched until a later query asks for them.

    Returns `(enriched_df_for_queried_cities, metadata_dict)`.
    """
    if query_type != "brand":
        frames = []
        sources: dict[str, str] = {}
        for city in cities:
            df, src = smart_fetch(brand, city)
            sources[city] = src
            if df is not None and not df.empty:
                frames.append(df)
        merged = pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()
        return merged, {
            "query_type": query_type,
            "queried_cities": cities,
            "per_city_sources": sources,
        }

    metadata: dict = {
        "queried_cities": cities,
        "stores_enriched_this_call": 0,
        "stores_from_cache": 0,
        "total_stores_in_db": 0,
        "stage1_ran": False,
        "stage1_records": 0,
        "stage2_errors": [],
    }

    _stage1_national_scrape(brand, metadata)

    _stage2_enrich_cities(brand, cities, metadata)

    metadata["total_stores_in_db"] = db.count_enriched_stores_for_brand(brand)

    df = _fetch_brand_city_rows(brand, cities)
    return df, metadata


def _stage1_national_scrape(brand: str, metadata: dict) -> None:
    """Run the registry-driven scrape once and persist results."""
    meta = db.get_brand_metadata(brand)
    ts = meta.get("full_scrape_completed_at") if meta else None
    if ts and (time.time() - float(ts)) < BRAND_METADATA_TTL_SEC:
        logger.info("[enrichment] stage1 skipped for %s (cached national scrape)", brand)
        return

    from src.fetchers import brand_scraper
    info = brand_scraper.get_brand_info(brand)
    if not info:
        logger.info("[enrichment] stage1 skipped: %s not in scraper registry", brand)
        return
    if info.get("extraction_method") == "blocked":
        logger.info("[enrichment] stage1 skipped: %s is marked blocked", brand)
        return

    try:
        df = brand_scraper.scrape_brand_stores(brand, cities=[])
    except Exception as e:
        logger.warning("[enrichment] stage1 scrape failed for %s: %s", brand, e)
        return

    metadata["stage1_ran"] = True
    if df is None or df.empty:
        metadata["stage1_records"] = 0
        return

    count = 0
    for rec in df.to_dict(orient="records"):
        rec.setdefault("brand", brand)
        rec.setdefault("source", "brand_website")
        db.upsert_store(rec)
        count += 1

    metadata["stage1_records"] = count
    db.upsert_brand_metadata(
        brand=brand,
        total_stores_estimate=count,
        source="full_scrape",
        confidence=1.0,
        full_scrape_completed_at=time.time(),
    )

    # Phase 5.4: mark brand verified in registry on successful scrape.
    try:
        db.upsert_brand_to_registry(
            canonical_name=brand,
            source="discovered_scraper",
            verified=1,
        )
    except Exception as e:
        logger.debug("registry verify after scrape failed: %s", e)


def _stage2_enrich_cities(brand: str, cities: list[str], metadata: dict) -> None:
    """Google Places enrichment for stores in `cities` that aren't fresh."""
    if not cities:
        return

    from src.fetchers import google_places

    if not settings.GOOGLE_PLACES_API_KEY:
        logger.info("[enrichment] stage2 skipped: GOOGLE_PLACES_API_KEY not set")
        return

    stale = db.get_unenriched_store_ids(brand, cities)
    if not stale:
        return

    enriched_count = 0

    for city in cities:
        try:
            records = google_places.search_text(brand, city)
        except Exception as e:
            logger.warning("[enrichment] stage2 Places error in %s: %s", city, e)
            metadata["stage2_errors"].append({"city": city, "error": str(e)})
            continue

        for rec in records:
            rec.setdefault("brand", brand)
            rec.setdefault("city", city)
            sid = db.upsert_store(rec)
            db.mark_store_enriched(sid, source="google_places")
            enriched_count += 1
        db.add_known_city_for_brand(brand, city)

    metadata["stores_enriched_this_call"] = enriched_count
    metadata["stores_from_cache"] = max(0, len(stale) - enriched_count)


def _fetch_brand_city_rows(brand: str, cities: list[str]) -> pd.DataFrame:
    """Pull stores from the DB for `brand` in `cities`, joined to latest rating."""
    if not cities:
        return pd.DataFrame()

    conn = db._get_conn()
    try:
        placeholders = ",".join("?" * len(cities))
        rows = conn.execute(
            f"""
            SELECT s.*,
                   r.rating        AS rating,
                   r.review_count  AS review_count,
                   r.fetched_at    AS rating_fetched_at
            FROM stores s
            LEFT JOIN (
                SELECT store_id, rating, review_count, fetched_at
                FROM store_ratings
                WHERE id IN (SELECT MAX(id) FROM store_ratings GROUP BY store_id)
            ) r ON r.store_id = s.store_id
            WHERE s.brand = ? AND s.city IN ({placeholders})
            """,
            [brand] + list(cities),
        ).fetchall()
    finally:
        conn.close()

    return pd.DataFrame([dict(r) for r in rows])


# ---------------------------------------------------------------------------
# Observability helpers (used by the Streamlit sidebar)
# ---------------------------------------------------------------------------

def invalidate(brand: str, city: str) -> None:
    """Drop Redis + DB entries for `(brand, city)` so the next call re-fetches."""
    client = _redis_client()
    if client is not None:
        try:
            client.delete(_query_redis_key(brand, city))
        except Exception as e:
            _mark_redis_down(e)
    try:
        conn = db._get_conn()
        try:
            conn.execute(
                "DELETE FROM query_cache WHERE query_hash = ?",
                (db._query_hash(brand, city),),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception as e:
        logger.warning("DB invalidate failed: %s", e)


def cache_stats() -> dict:
    """Roll-up suitable for the sidebar: DB counts + cumulative cost."""
    return {
        "db": db.db_stats(),
        "api_cost": db.cumulative_api_cost(),
        "redis_available": _redis_ok(),
        "sources": {
            "google_places": bool(settings.GOOGLE_PLACES_API_KEY),
            "serper": bool(settings.SERPER_API_KEY),
        },
    }
