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
import logging
from typing import Callable, Optional, Tuple

import pandas as pd

from src.core import config
from src.core import db
from src.core import redis_cache

logger = logging.getLogger(__name__)

_redis_available: Optional[bool] = None


def _redis_ok() -> bool:
    global _redis_available
    if _redis_available is False:
        return False
    return True


def _mark_redis_down(err: Exception) -> None:
    global _redis_available
    if _redis_available is not False:
        logger.warning("Redis unavailable (%s); falling back to SQLite cache.", err)
    _redis_available = False


def get_cached(
    brand: str,
    city: str,
    source: str = "",
) -> Optional[pd.DataFrame]:
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
    ttl: Optional[int] = None,
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
    """Return a live Redis client or None if unavailable (best-effort)."""
    if _redis_available is False:
        return None
    try:
        return redis_cache._get_client()
    except Exception as e:
        _mark_redis_down(e)
        return None


def _redis_get_query(brand: str, city: str) -> Optional[pd.DataFrame]:
    client = _redis_client()
    if client is None:
        return None
    try:
        import json as _json
        payload = client.get(_query_redis_key(brand, city))
        if payload is None:
            return None
        records = _json.loads(payload)
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
    live_fetcher: Optional[Callable[[str, str], pd.DataFrame]] = None,
    reconciler: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None,
    mock_fetcher: Optional[Callable[[str, str], pd.DataFrame]] = None,
    max_db_age: int = db.QUERY_TTL_DEFAULT,
) -> Tuple[pd.DataFrame, str]:
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
    # ---- Layer 1: Redis ----
    redis_df = _redis_get_query(brand, city)
    if redis_df is not None and not redis_df.empty:
        return redis_df, "redis"

    # ---- Layer 2: persistent DB ----
    try:
        db_df = db.lookup_query(brand, city, max_age=max_db_age)
    except Exception as e:
        logger.warning("DB query_cache lookup failed: %s", e)
        db_df = None
    if db_df is not None and not db_df.empty:
        _redis_set_query(brand, city, db_df)
        return db_df, "db"

    # ---- Layer 3: live APIs ----
    if live_fetcher is None or reconciler is None:
        from src.fetchers import multi_fetcher
        from src.analysis import reconciler as _rec
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
            # Persist to DB + Redis so the next call is free.
            store_ids = []
            for rec in merged.to_dict(orient="records"):
                rec.setdefault("brand", brand)
                rec.setdefault("city", city)
                store_ids.append(db.upsert_store(rec))
            db.save_query_result(brand, city, store_ids, source="api")
            _redis_set_query(brand, city, merged)
            return merged, "api"

    # ---- Layer 4: last-resort mock (only if everything above was empty) ----
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
        "redis_available": _redis_available is not False,
        "sources": {
            "google_places": bool(config.GOOGLE_PLACES_API_KEY),
            "serper": bool(config.SERPER_API_KEY),
        },
    }
