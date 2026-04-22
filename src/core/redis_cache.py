"""
Redis-backed cache for multi-source fetcher responses.

Keys are `loc_intel:{brand}|{city}|{source}`; values are JSON-encoded
DataFrames (orient="records"). TTL defaults to 7 days.

Connection errors are raised to the caller so `cache_manager` can fall
back to SQLite.
"""
from __future__ import annotations

import json

import pandas as pd

from src.core.config import REDIS_URL

DEFAULT_TTL = 7 * 24 * 3600

SOURCE_TTLS = {
    "brand_website": 20 * 24 * 3600,  # 20 days - store locator pages change slowly
    "google_places": 24 * 3600,       # 1 day - ratings drift daily
    "serper": 24 * 3600,               # 1 day - Google ratings drift daily
    "osm": 30 * 24 * 3600,             # 30 days - OSM POIs are very stable
    "outscraper": 24 * 3600,           # 1 day - review feed churns
}


def ttl_for(source: str) -> int:
    """TTL in seconds for a given source, defaulting to DEFAULT_TTL."""
    return SOURCE_TTLS.get((source or "").lower().strip(), DEFAULT_TTL)


_KEY_PREFIX = "loc_intel:"

_client = None


def _get_client():
    global _client
    if _client is not None:
        return _client
    try:
        import redis
    except ImportError as e:
        raise RuntimeError(
            "redis package not installed; pip install redis>=5.0.0"
        ) from e
    _client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    _client.ping()
    return _client


def _cache_key(brand: str, city: str, source: str = "") -> str:
    parts = [brand.lower().strip(), city.lower().strip()]
    if source:
        parts.append(source.lower().strip())
    return _KEY_PREFIX + "|".join(parts)


def get_cached(
    brand: str,
    city: str,
    source: str = "",
) -> pd.DataFrame | None:
    """
    Return the cached DataFrame for (brand, city, source) or None.

    Raises on Redis connection errors so the caller can fall back to another
    cache tier.
    """
    client = _get_client()
    payload = client.get(_cache_key(brand, city, source))
    if payload is None:
        return None
    try:
        records = json.loads(payload)
    except json.JSONDecodeError:
        return None
    return pd.DataFrame(records) if records else pd.DataFrame()


def set_cached(
    brand: str,
    city: str,
    source: str,
    df: pd.DataFrame,
    ttl: int | None = None,
) -> None:
    """
    Store a DataFrame under (brand, city, source). If `ttl` is not supplied,
    the per-source default from SOURCE_TTLS is used.
    """
    client = _get_client()
    if df is None or df.empty:
        payload = "[]"
    else:
        payload = df.to_json(orient="records", date_format="iso")
    effective_ttl = ttl if ttl is not None else ttl_for(source)
    client.setex(_cache_key(brand, city, source), effective_ttl, payload)
