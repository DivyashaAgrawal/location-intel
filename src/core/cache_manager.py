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
from collections.abc import Callable

import pandas as pd

from src.core import config, db, redis_cache

logger = logging.getLogger(__name__)

_redis_available: bool | None = None


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
    """Return a live Redis client or None if unavailable (best-effort)."""
    if _redis_available is False:
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
        from src.analysis import reconciler as _rec
        from src.fetchers import multi_fetcher
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
        # Category queries still use the simple per-city path.
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

    from src.core import db as _db
    metadata["total_stores_in_db"] = _db.count_enriched_stores_for_brand(brand)

    df = _fetch_brand_city_rows(brand, cities)
    return df, metadata


def _stage1_national_scrape(brand: str, metadata: dict) -> None:
    """Run the registry-driven scrape once and persist results.

    Skipped if a recent full_scrape_completed_at exists in brand_metadata.
    """
    from src.core import db as _db

    meta = _db.get_brand_metadata(brand)
    ts = meta.get("full_scrape_completed_at") if meta else None
    if ts and (_time_now() - float(ts)) < BRAND_METADATA_TTL_SEC:
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
        _db.upsert_store(rec)
        count += 1

    metadata["stage1_records"] = count
    _db.upsert_brand_metadata(
        brand=brand,
        total_stores_estimate=count,
        source="full_scrape",
        confidence=1.0,
        full_scrape_completed_at=_time_now(),
    )


def _stage2_enrich_cities(brand: str, cities: list[str], metadata: dict) -> None:
    """Google Places enrichment for stores in `cities` that aren't fresh."""
    if not cities:
        return

    from src.core import db as _db
    from src.fetchers import google_places

    if not config.GOOGLE_PLACES_API_KEY:
        logger.info("[enrichment] stage2 skipped: GOOGLE_PLACES_API_KEY not set")
        return

    stale = _db.get_unenriched_store_ids(brand, cities)
    if not stale:
        return

    enriched_count = 0
    cached_count = 0

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
            sid = _db.upsert_store(rec)
            _db.mark_store_enriched(sid, source="google_places")
            enriched_count += 1
        _db.add_known_city_for_brand(brand, city)

    metadata["stores_enriched_this_call"] = enriched_count
    metadata["stores_from_cache"] = max(0, len(stale) - enriched_count)
    cached_count += metadata["stores_from_cache"]


def _fetch_brand_city_rows(brand: str, cities: list[str]) -> pd.DataFrame:
    """Pull stores from the DB for `brand` in `cities`, joined to latest rating."""
    from src.core import db as _db

    if not cities:
        return pd.DataFrame()

    conn = _db._get_conn()
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


def _time_now() -> float:
    import time as _time
    return _time.time()


def estimate_enrichment_needed(brand: str, cities: list[str]) -> int:
    """
    Project how many Google Places enrichment calls running the query now
    would cost. Subtracts stores already freshly enriched in the DB.

    Assumptions:
      - With no `cities` filter (or "all India"), the projection is the
        brand's full store count.
      - With a city filter, we approximate per-city share as
        min(len(cities)/20, 1.0) of the national count (tier-1 cities
        hold roughly 5-10% of a national footprint).
      - Stores already enriched and still fresh (inside ENRICHMENT_TTL_SEC)
        don't count as projected calls.
    """
    from src.core import db as _db

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

    already = _db.count_enriched_stores_for_brand(brand, cities or None)
    return max(0, int(projected) - int(already))


def get_already_enriched_cities(brand: str) -> list[dict]:
    """Return [{city, store_count}] for cities with any enriched stores."""
    from src.core import db as _db

    conn = _db._get_conn()
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


# ---------------------------------------------------------------------------
# Brand size estimation (Phase 1.5)
# ---------------------------------------------------------------------------

BRAND_METADATA_TTL_SEC = 90 * 24 * 3600  # 90 days


def estimate_brand_size(
    brand: str,
    force_refresh: bool = False,
) -> dict:
    """
    Fast, cheap store-count estimate. Decision tree:

      1. Cache hit (<90 days old, not forced)       -> return cached.
      2. Brand has `headline_count_selector`        -> Playwright headline read.
      3. Brand in registry (no headline)            -> full scrape, count.
      4. Brand NOT in registry                      -> Places Text Search,
                                                       paginated up to 3 pages.
      5. All methods fail                           -> count=None, conf=0.

    Returns a dict with:
      brand, total_stores_estimate, source, confidence, last_refreshed,
      known_cities, coverage_pct, is_stale, fetch_latency_ms.
    """
    import time as _time

    start = _time.time()

    if not force_refresh:
        cached = db.get_brand_metadata(brand)
        if cached and cached.get("last_refreshed"):
            age = _time.time() - float(cached["last_refreshed"])
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

    latency_ms = int((_time.time() - start) * 1000)

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
        "last_refreshed": _time.time(),
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
    """Run the full registry-driven scrape once; count results as the size."""
    import time as _time
    try:
        from src.fetchers import brand_scraper
        df = brand_scraper.scrape_brand_stores(brand, cities=[])
    except Exception as e:
        logger.info("[brand_size] full scrape failed for %s: %s", brand, e)
        return None, None
    if df is None or df.empty:
        return None, None
    return int(len(df)), _time.time()


def _estimate_via_places_pagination(brand: str) -> tuple[int | None, bool]:
    """
    Fallback for brands absent from the scraper registry: paginate Google
    Places nationally and use the total row count as a size estimate.

    Returns (count_or_None, hit_pagination_cap). `hit_pagination_cap` flags
    that the true count is >= returned value (Google caps at 60 rows via
    3 pages of 20).
    """
    try:
        from src.fetchers import google_places
    except Exception:
        return None, False

    seen: set[str] = set()
    total_rows = 0
    pages_seen = 0
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
        pages_seen += 1
        if len(records) >= 60:
            hit_cap = True

    if total_rows == 0:
        return None, False
    return total_rows, hit_cap


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
