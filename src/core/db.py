"""
Persistent store database.

One SQLite file holds everything cache-and-query related. Five tables:

- `stores`              : canonical store rows (one per physical location)
- `store_ratings`       : append-only rating/review snapshots per store
- `query_cache`         : maps (brand, city) queries to the store_ids that satisfied them
- `source_cache`        : per-source raw adapter blobs keyed by (brand, city, source),
                          used by multi_fetcher to short-circuit repeat adapter calls
- `api_call_log`        : every API call with source, timestamp, and cost
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import time
from typing import Any, Iterable, Optional

import pandas as pd


DEFAULT_DB_PATH = os.environ.get(
    "LOCATION_INTEL_DB_PATH",
    os.path.join(os.path.dirname(os.path.dirname(__file__)), "location_intel.db"),
)

QUERY_TTL_DEFAULT = 24 * 3600  # 1 day


_SCHEMA = """
CREATE TABLE IF NOT EXISTS stores (
    store_id        TEXT PRIMARY KEY,
    place_id        TEXT,
    brand           TEXT NOT NULL,
    title           TEXT,
    address         TEXT,
    city            TEXT,
    state           TEXT,
    pincode         TEXT,
    latitude        REAL,
    longitude       REAL,
    phone           TEXT,
    website         TEXT,
    category        TEXT,
    sources         TEXT,
    source_count    INTEGER,
    data_quality    REAL,
    last_updated    REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_stores_brand_city ON stores(brand, city);
CREATE INDEX IF NOT EXISTS ix_stores_pincode ON stores(pincode);
CREATE INDEX IF NOT EXISTS ix_stores_place_id ON stores(place_id);

CREATE TABLE IF NOT EXISTS store_ratings (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    store_id        TEXT NOT NULL,
    rating          REAL,
    review_count    INTEGER,
    fetched_at      REAL NOT NULL,
    FOREIGN KEY (store_id) REFERENCES stores(store_id)
);

CREATE INDEX IF NOT EXISTS ix_ratings_store ON store_ratings(store_id, fetched_at);

CREATE TABLE IF NOT EXISTS query_cache (
    query_hash      TEXT PRIMARY KEY,
    brand           TEXT NOT NULL,
    city            TEXT NOT NULL,
    store_ids       TEXT NOT NULL,      -- JSON list of store_ids
    source          TEXT,                -- which upstream source last populated this row
    fetched_at      REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_query_cache_brand_city ON query_cache(brand, city);

CREATE TABLE IF NOT EXISTS source_cache (
    brand           TEXT NOT NULL,
    city            TEXT NOT NULL,
    source          TEXT NOT NULL,
    data            TEXT NOT NULL,       -- JSON list of records
    fetched_at      REAL NOT NULL,
    PRIMARY KEY (brand, city, source)
);

CREATE TABLE IF NOT EXISTS api_call_log (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    source                  TEXT NOT NULL,
    brand                   TEXT,
    city                    TEXT,
    success                 INTEGER NOT NULL,
    estimated_cost_usd      REAL NOT NULL DEFAULT 0,
    called_at               REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_api_log_source ON api_call_log(source, called_at);
"""

def _get_conn(db_path: Optional[str] = None) -> sqlite3.Connection:
    path = db_path or DEFAULT_DB_PATH
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.executescript(_SCHEMA)
    return conn


def init_db(db_path: Optional[str] = None) -> None:
    """Create tables and indexes if they don't exist. Safe to call repeatedly."""
    conn = _get_conn(db_path)
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Store identity
# ---------------------------------------------------------------------------

def compute_store_id(
    place_id: Optional[str] = None,
    brand: Optional[str] = None,
    latitude: Optional[float] = None,
    longitude: Optional[float] = None,
) -> str:
    """
    Deterministic store identifier.

    If a Google `place_id` is present, trust it (prefixed `g:`).
    Otherwise hash `(brand-lower, lat-6dp, lng-6dp)` so the same physical
    store from different sources collapses to the same id.
    """
    if place_id:
        return f"g:{place_id}"
    brand_key = (brand or "").strip().lower()
    lat_key = f"{float(latitude):.6f}" if latitude is not None else "nan"
    lng_key = f"{float(longitude):.6f}" if longitude is not None else "nan"
    payload = f"{brand_key}|{lat_key}|{lng_key}".encode("utf-8")
    return "h:" + hashlib.sha1(payload).hexdigest()[:16]


def _query_hash(brand: str, city: str) -> str:
    key = f"{brand.strip().lower()}|{city.strip().lower()}".encode("utf-8")
    return hashlib.sha1(key).hexdigest()


# ---------------------------------------------------------------------------
# Store upsert / lookup
# ---------------------------------------------------------------------------

_STORE_COLS = [
    "store_id", "place_id", "brand", "title", "address", "city", "state",
    "pincode", "latitude", "longitude", "phone", "website", "category",
    "sources", "source_count", "data_quality",
]


def upsert_store(record: dict, db_path: Optional[str] = None) -> str:
    """
    Insert or update a single store row. Returns the store_id used.

    The record should carry at least `brand`; `place_id` is preferred for the
    id derivation, lat/lng is the fallback.
    """
    conn = _get_conn(db_path)
    try:
        sid = record.get("store_id") or compute_store_id(
            place_id=record.get("place_id"),
            brand=record.get("brand"),
            latitude=record.get("latitude"),
            longitude=record.get("longitude"),
        )
        row = {c: record.get(c) for c in _STORE_COLS}
        row["store_id"] = sid
        row["last_updated"] = time.time()

        placeholders = ", ".join(["?"] * (len(_STORE_COLS) + 1))
        columns = ", ".join(_STORE_COLS + ["last_updated"])
        updates = ", ".join(
            f"{c}=excluded.{c}" for c in _STORE_COLS + ["last_updated"] if c != "store_id"
        )
        conn.execute(
            f"""
            INSERT INTO stores ({columns}) VALUES ({placeholders})
            ON CONFLICT(store_id) DO UPDATE SET {updates}
            """,
            [row[c] for c in _STORE_COLS] + [row["last_updated"]],
        )

        rating = record.get("rating")
        review_count = record.get("review_count")
        if rating is not None or review_count is not None:
            conn.execute(
                "INSERT INTO store_ratings (store_id, rating, review_count, fetched_at) VALUES (?, ?, ?, ?)",
                (sid, rating, review_count, time.time()),
            )

        conn.commit()
        return sid
    finally:
        conn.close()


def upsert_stores(records: Iterable[dict], db_path: Optional[str] = None) -> list[str]:
    """Bulk upsert. Returns the list of store_ids written, preserving input order."""
    ids = []
    for r in records:
        ids.append(upsert_store(r, db_path=db_path))
    return ids


def get_stores_by_ids(
    ids: list[str], db_path: Optional[str] = None
) -> pd.DataFrame:
    """Bulk fetch stores by id, joined with their latest rating row."""
    if not ids:
        return pd.DataFrame()
    conn = _get_conn(db_path)
    try:
        placeholders = ",".join("?" * len(ids))
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
                WHERE id IN (
                    SELECT MAX(id) FROM store_ratings GROUP BY store_id
                )
            ) r ON r.store_id = s.store_id
            WHERE s.store_id IN ({placeholders})
            """,
            ids,
        ).fetchall()
        return pd.DataFrame([dict(r) for r in rows])
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Query-level cache
# ---------------------------------------------------------------------------

def save_query_result(
    brand: str,
    city: str,
    store_ids: list[str],
    source: str,
    db_path: Optional[str] = None,
) -> None:
    """Record that `(brand, city)` currently resolves to `store_ids` (from `source`)."""
    conn = _get_conn(db_path)
    try:
        qh = _query_hash(brand, city)
        conn.execute(
            """
            INSERT OR REPLACE INTO query_cache
              (query_hash, brand, city, store_ids, source, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (qh, brand, city, json.dumps(store_ids), source, time.time()),
        )
        conn.commit()
    finally:
        conn.close()


def lookup_query(
    brand: str,
    city: str,
    max_age: int = QUERY_TTL_DEFAULT,
    db_path: Optional[str] = None,
) -> Optional[pd.DataFrame]:
    """
    Look up a prior `(brand, city)` result. Returns a DataFrame of stores
    if the cached entry exists and is within `max_age` seconds; else None.
    """
    conn = _get_conn(db_path)
    try:
        qh = _query_hash(brand, city)
        row = conn.execute(
            "SELECT store_ids, fetched_at FROM query_cache WHERE query_hash = ?",
            (qh,),
        ).fetchone()
        if not row:
            return None
        if time.time() - row["fetched_at"] > max_age:
            return None
        ids = json.loads(row["store_ids"])
    finally:
        conn.close()

    if not ids:
        return pd.DataFrame()
    return get_stores_by_ids(ids, db_path=db_path)


# ---------------------------------------------------------------------------
# Per-source adapter cache
# ---------------------------------------------------------------------------

def _source_cache_key(brand: str, city: str, source: str) -> tuple[str, str, str]:
    return (
        (brand or "").strip().lower(),
        (city or "").strip().lower(),
        (source or "").strip().lower(),
    )


def get_source_cache(
    brand: str,
    city: str,
    source: str,
    ttl: int,
    db_path: Optional[str] = None,
) -> Optional[pd.DataFrame]:
    """Return the cached adapter DataFrame if present and within `ttl` seconds."""
    b, c, s = _source_cache_key(brand, city, source)
    conn = _get_conn(db_path)
    try:
        row = conn.execute(
            "SELECT data, fetched_at FROM source_cache "
            "WHERE brand = ? AND city = ? AND source = ?",
            (b, c, s),
        ).fetchone()
    finally:
        conn.close()

    if row is None:
        return None
    if time.time() - row["fetched_at"] > ttl:
        return None

    records = json.loads(row["data"])
    return pd.DataFrame(records) if records else pd.DataFrame()


def set_source_cache(
    brand: str,
    city: str,
    source: str,
    df: pd.DataFrame,
    db_path: Optional[str] = None,
) -> None:
    """Upsert the adapter DataFrame for (brand, city, source)."""
    b, c, s = _source_cache_key(brand, city, source)
    records = df.to_dict(orient="records") if df is not None and not df.empty else []
    conn = _get_conn(db_path)
    try:
        conn.execute(
            """
            INSERT INTO source_cache (brand, city, source, data, fetched_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(brand, city, source) DO UPDATE SET
                data = excluded.data,
                fetched_at = excluded.fetched_at
            """,
            (b, c, s, json.dumps(records, default=str), time.time()),
        )
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# API call log
# ---------------------------------------------------------------------------

# Per-call cost estimates in USD. Source-keyed; update as pricing changes.
# Google Places v1 text-search with a field mask: ~$32 / 1000 calls = $0.032.
# Serper.dev: free-tier queries are free but accounted as $0 here; after the
# free tier it's $0.003 / call. Default to post-free-tier.
SOURCE_COST_USD = {
    "google_places": 0.032,
    "serper": 0.003,
    "osm": 0.0,
    "brand_website": 0.0,
    "outscraper": 0.01,
    "nominatim": 0.0,
    "mock": 0.0,
}


def log_api_call(
    source: str,
    brand: Optional[str] = None,
    city: Optional[str] = None,
    success: bool = True,
    cost: Optional[float] = None,
    db_path: Optional[str] = None,
) -> None:
    """Record a single API call. `cost` defaults to the SOURCE_COST_USD entry."""
    if cost is None:
        cost = SOURCE_COST_USD.get(source, 0.0)
    conn = _get_conn(db_path)
    try:
        conn.execute(
            """
            INSERT INTO api_call_log (source, brand, city, success, estimated_cost_usd, called_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (source, brand, city, 1 if success else 0, float(cost), time.time()),
        )
        conn.commit()
    finally:
        conn.close()


def cumulative_api_cost(db_path: Optional[str] = None) -> dict[str, Any]:
    """Return {total_usd, by_source: {...}, total_calls}."""
    conn = _get_conn(db_path)
    try:
        total_row = conn.execute(
            "SELECT COUNT(*) AS n, COALESCE(SUM(estimated_cost_usd), 0) AS total FROM api_call_log"
        ).fetchone()
        by_source = conn.execute(
            """
            SELECT source,
                   COUNT(*) AS calls,
                   COALESCE(SUM(estimated_cost_usd), 0) AS cost_usd
            FROM api_call_log
            GROUP BY source
            ORDER BY cost_usd DESC
            """
        ).fetchall()
    finally:
        conn.close()

    return {
        "total_calls": total_row["n"],
        "total_usd": round(total_row["total"], 4),
        "by_source": [
            {"source": r["source"], "calls": r["calls"], "cost_usd": round(r["cost_usd"], 4)}
            for r in by_source
        ],
    }


def db_stats(db_path: Optional[str] = None) -> dict[str, Any]:
    """Counts across the core tables. Cheap; safe to call from the UI."""
    conn = _get_conn(db_path)
    try:
        def _count(table: str) -> int:
            return conn.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()["n"]
        return {
            "stores": _count("stores"),
            "store_ratings": _count("store_ratings"),
            "query_cache_entries": _count("query_cache"),
            "api_calls": _count("api_call_log"),
        }
    finally:
        conn.close()
