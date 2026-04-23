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
from collections.abc import Iterable
from typing import Any

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
    sources             TEXT,
    source_count        INTEGER,
    data_quality        REAL,
    last_updated        REAL NOT NULL,
    enriched_at         REAL,               -- NULL if never enriched
    enrichment_source   TEXT                -- e.g. 'google_places', 'serper'
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

CREATE TABLE IF NOT EXISTS brand_metadata (
    brand                       TEXT PRIMARY KEY,
    total_stores_estimate       INTEGER,
    total_stores_source         TEXT,           -- 'scraper_headline' | 'full_scrape' | 'places_pagination_cap' | 'manual'
    estimate_confidence         REAL,           -- 0.0 .. 1.0
    last_refreshed              REAL NOT NULL,
    known_cities_json           TEXT,           -- JSON list of cities we've enriched so far
    full_scrape_completed_at    REAL
);

CREATE INDEX IF NOT EXISTS idx_brand_metadata_refreshed ON brand_metadata(last_refreshed DESC);

CREATE TABLE IF NOT EXISTS discovered_competitors (
    brand                TEXT PRIMARY KEY,
    category             TEXT NOT NULL,
    first_seen           REAL NOT NULL,
    last_seen            REAL NOT NULL,
    times_seen           INTEGER NOT NULL DEFAULT 1,
    source               TEXT NOT NULL,
    manually_verified    INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_discovered_category ON discovered_competitors(category, last_seen DESC);

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

CREATE TABLE IF NOT EXISTS brand_registry (
    brand_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    canonical_name   TEXT NOT NULL UNIQUE,
    aliases_json     TEXT,
    category         TEXT,
    source           TEXT NOT NULL,
    times_queried    INTEGER DEFAULT 0,
    verified         INTEGER DEFAULT 0,
    added_at         REAL NOT NULL,
    last_seen_at     REAL
);

CREATE INDEX IF NOT EXISTS idx_brand_category ON brand_registry(category);
CREATE INDEX IF NOT EXISTS idx_brand_source ON brand_registry(source);
CREATE INDEX IF NOT EXISTS idx_brand_name_lower ON brand_registry(LOWER(canonical_name));
"""

def _get_conn(db_path: str | None = None) -> sqlite3.Connection:
    path = db_path or DEFAULT_DB_PATH
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.executescript(_SCHEMA)
    _apply_migrations(conn)
    return conn


def _apply_migrations(conn: sqlite3.Connection) -> None:
    """Additive, idempotent column migrations for existing DB files."""
    existing = {row["name"] for row in conn.execute("PRAGMA table_info(stores)").fetchall()}
    if "enriched_at" not in existing:
        conn.execute("ALTER TABLE stores ADD COLUMN enriched_at REAL")
    if "enrichment_source" not in existing:
        conn.execute("ALTER TABLE stores ADD COLUMN enrichment_source TEXT")
    conn.commit()


def init_db(db_path: str | None = None) -> None:
    """Create tables and indexes if they don't exist. Safe to call repeatedly."""
    conn = _get_conn(db_path)
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Store identity
# ---------------------------------------------------------------------------

def compute_store_id(
    place_id: str | None = None,
    brand: str | None = None,
    latitude: float | None = None,
    longitude: float | None = None,
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
    payload = f"{brand_key}|{lat_key}|{lng_key}".encode()
    return "h:" + hashlib.sha1(payload).hexdigest()[:16]


def _query_hash(brand: str, city: str) -> str:
    key = f"{brand.strip().lower()}|{city.strip().lower()}".encode()
    return hashlib.sha1(key).hexdigest()


# ---------------------------------------------------------------------------
# Store upsert / lookup
# ---------------------------------------------------------------------------

_STORE_COLS = [
    "store_id", "place_id", "brand", "title", "address", "city", "state",
    "pincode", "latitude", "longitude", "phone", "website", "category",
    "sources", "source_count", "data_quality",
    "enriched_at", "enrichment_source",
]

ENRICHMENT_TTL_SEC = 7 * 24 * 3600  # 7 days: RATING_TTL


def upsert_store(record: dict, db_path: str | None = None) -> str:
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


def upsert_stores(records: Iterable[dict], db_path: str | None = None) -> list[str]:
    """Bulk upsert. Returns the list of store_ids written, preserving input order."""
    ids = []
    for r in records:
        ids.append(upsert_store(r, db_path=db_path))
    return ids


def get_stores_by_ids(
    ids: list[str], db_path: str | None = None
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
    db_path: str | None = None,
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
    db_path: str | None = None,
) -> pd.DataFrame | None:
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
    db_path: str | None = None,
) -> pd.DataFrame | None:
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
    db_path: str | None = None,
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
    brand: str | None = None,
    city: str | None = None,
    success: bool = True,
    cost: float | None = None,
    db_path: str | None = None,
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


def cumulative_api_cost(db_path: str | None = None) -> dict[str, Any]:
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


# ---------------------------------------------------------------------------
# Brand metadata (Phase 1.5)
# ---------------------------------------------------------------------------

def _brand_key(brand: str) -> str:
    return (brand or "").strip()


def get_brand_metadata(
    brand: str, db_path: str | None = None
) -> dict[str, Any] | None:
    """Return the stored brand_metadata row as a dict, or None."""
    conn = _get_conn(db_path)
    try:
        row = conn.execute(
            "SELECT * FROM brand_metadata WHERE brand = ?",
            (_brand_key(brand),),
        ).fetchone()
    finally:
        conn.close()
    if not row:
        return None
    d = dict(row)
    try:
        d["known_cities"] = json.loads(d.get("known_cities_json") or "[]")
    except Exception:
        d["known_cities"] = []
    return d


def upsert_brand_metadata(
    brand: str,
    total_stores_estimate: int | None,
    source: str,
    confidence: float,
    known_cities: list[str] | None = None,
    full_scrape_completed_at: float | None = None,
    db_path: str | None = None,
) -> None:
    """Insert or update a brand_metadata row. `last_refreshed` is stamped now."""
    conn = _get_conn(db_path)
    try:
        existing = conn.execute(
            "SELECT known_cities_json, full_scrape_completed_at "
            "FROM brand_metadata WHERE brand = ?",
            (_brand_key(brand),),
        ).fetchone()

        if known_cities is None and existing is not None:
            try:
                known_cities = json.loads(existing["known_cities_json"] or "[]")
            except Exception:
                known_cities = []
        cities_json = json.dumps(sorted(set(known_cities or [])))

        if full_scrape_completed_at is None and existing is not None:
            full_scrape_completed_at = existing["full_scrape_completed_at"]

        conn.execute(
            """
            INSERT INTO brand_metadata
              (brand, total_stores_estimate, total_stores_source,
               estimate_confidence, last_refreshed, known_cities_json,
               full_scrape_completed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(brand) DO UPDATE SET
              total_stores_estimate = excluded.total_stores_estimate,
              total_stores_source   = excluded.total_stores_source,
              estimate_confidence   = excluded.estimate_confidence,
              last_refreshed        = excluded.last_refreshed,
              known_cities_json     = excluded.known_cities_json,
              full_scrape_completed_at = excluded.full_scrape_completed_at
            """,
            (
                _brand_key(brand),
                int(total_stores_estimate) if total_stores_estimate is not None else None,
                source,
                float(confidence),
                time.time(),
                cities_json,
                full_scrape_completed_at,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def add_known_city_for_brand(
    brand: str, city: str, db_path: str | None = None
) -> None:
    """Append `city` to the brand's known_cities list (idempotent)."""
    meta = get_brand_metadata(brand, db_path=db_path)
    known = set(meta.get("known_cities", [])) if meta else set()
    if city and city not in known:
        known.add(city)
        upsert_brand_metadata(
            brand=brand,
            total_stores_estimate=(meta or {}).get("total_stores_estimate"),
            source=(meta or {}).get("total_stores_source") or "manual",
            confidence=(meta or {}).get("estimate_confidence") or 0.0,
            known_cities=sorted(known),
            db_path=db_path,
        )


def mark_store_enriched(
    store_id: str,
    source: str = "google_places",
    db_path: str | None = None,
) -> None:
    """Stamp a store as enriched by `source` as of now."""
    conn = _get_conn(db_path)
    try:
        conn.execute(
            "UPDATE stores SET enriched_at = ?, enrichment_source = ? WHERE store_id = ?",
            (time.time(), source, store_id),
        )
        conn.commit()
    finally:
        conn.close()


def get_unenriched_store_ids(
    brand: str,
    cities: list[str],
    ttl_sec: int = ENRICHMENT_TTL_SEC,
    db_path: str | None = None,
) -> list[dict]:
    """
    Return stores for `brand` in `cities` that lack a fresh enrichment stamp.

    Stale = `enriched_at IS NULL` or older than `ttl_sec`. Each row carries
    enough context to run an enrichment call (title + address + city).
    """
    if not cities:
        return []
    cutoff = time.time() - ttl_sec
    conn = _get_conn(db_path)
    try:
        placeholders = ",".join("?" * len(cities))
        rows = conn.execute(
            f"""
            SELECT store_id, brand, title, address, city, latitude, longitude
            FROM stores
            WHERE brand = ?
              AND city IN ({placeholders})
              AND (enriched_at IS NULL OR enriched_at < ?)
            """,
            [brand] + list(cities) + [cutoff],
        ).fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows]


def count_enriched_stores_for_brand(
    brand: str,
    cities: list[str] | None = None,
    db_path: str | None = None,
) -> int:
    """Store rows for `brand` (optionally within `cities`)."""
    conn = _get_conn(db_path)
    try:
        if cities:
            placeholders = ",".join("?" * len(cities))
            row = conn.execute(
                f"SELECT COUNT(*) AS n FROM stores "
                f"WHERE brand = ? AND city IN ({placeholders})",
                [brand] + list(cities),
            ).fetchone()
        else:
            row = conn.execute(
                "SELECT COUNT(*) AS n FROM stores WHERE brand = ?",
                (brand,),
            ).fetchone()
    finally:
        conn.close()
    return int(row["n"]) if row else 0


# ---------------------------------------------------------------------------
# Discovered competitors (Phase 3)
# ---------------------------------------------------------------------------

def record_discovered_competitor(
    brand: str,
    category: str,
    source: str = "category_query",
    db_path: str | None = None,
) -> None:
    """Idempotent upsert: first-seen stamps now; subsequent calls bump last_seen + times_seen."""
    brand = (brand or "").strip()
    category = (category or "").strip()
    if not brand or not category:
        return
    now = time.time()
    conn = _get_conn(db_path)
    try:
        conn.execute(
            """
            INSERT INTO discovered_competitors
              (brand, category, first_seen, last_seen, times_seen, source)
            VALUES (?, ?, ?, ?, 1, ?)
            ON CONFLICT(brand) DO UPDATE SET
              last_seen = excluded.last_seen,
              times_seen = discovered_competitors.times_seen + 1
            """,
            (brand, category, now, now, source),
        )
        conn.commit()
    finally:
        conn.close()


def get_discovered_competitors(
    category: str, db_path: str | None = None
) -> list[dict[str, Any]]:
    """Return all rows for `category`, ordered by manually_verified DESC, times_seen DESC."""
    conn = _get_conn(db_path)
    try:
        rows = conn.execute(
            """
            SELECT brand, category, first_seen, last_seen, times_seen,
                   source, manually_verified
            FROM discovered_competitors
            WHERE LOWER(category) = LOWER(?)
            ORDER BY manually_verified DESC, times_seen DESC, last_seen DESC
            """,
            (category,),
        ).fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows]


def verify_discovered_competitor(brand: str, db_path: str | None = None) -> None:
    conn = _get_conn(db_path)
    try:
        conn.execute(
            "UPDATE discovered_competitors SET manually_verified = 1 WHERE brand = ?",
            (brand,),
        )
        conn.commit()
    finally:
        conn.close()


def delete_discovered_competitor(brand: str, db_path: str | None = None) -> None:
    conn = _get_conn(db_path)
    try:
        conn.execute("DELETE FROM discovered_competitors WHERE brand = ?", (brand,))
        conn.commit()
    finally:
        conn.close()


def list_all_discovered_competitors(db_path: str | None = None) -> list[dict[str, Any]]:
    conn = _get_conn(db_path)
    try:
        rows = conn.execute(
            """
            SELECT brand, category, first_seen, last_seen, times_seen,
                   source, manually_verified
            FROM discovered_competitors
            ORDER BY times_seen DESC, last_seen DESC
            """
        ).fetchall()
    finally:
        conn.close()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Brand registry (Phase 5)
# ---------------------------------------------------------------------------

def upsert_brand_to_registry(
    canonical_name: str,
    aliases: list[str] | None = None,
    category: str | None = None,
    source: str = "manual",
    verified: int | bool = 0,
    db_path: str | None = None,
) -> int | None:
    """
    Insert or update a row in `brand_registry`.

    First-write stamps `added_at` now. Re-runs update `last_seen_at`, merge aliases,
    keep the highest-trust `source`/`verified` values already recorded, and only
    overwrite `category` when the new value is non-empty.
    """
    name = (canonical_name or "").strip()
    if not name:
        return None
    now = time.time()
    verified_flag = 1 if int(bool(verified)) else 0
    conn = _get_conn(db_path)
    try:
        row = conn.execute(
            "SELECT brand_id, aliases_json, category, source, verified "
            "FROM brand_registry WHERE LOWER(canonical_name) = LOWER(?)",
            (name,),
        ).fetchone()

        incoming_aliases = {a.strip() for a in (aliases or []) if a and a.strip()}

        if row is None:
            aliases_json = json.dumps(sorted(incoming_aliases)) if incoming_aliases else None
            cur = conn.execute(
                """
                INSERT INTO brand_registry
                  (canonical_name, aliases_json, category, source,
                   times_queried, verified, added_at, last_seen_at)
                VALUES (?, ?, ?, ?, 0, ?, ?, ?)
                """,
                (name, aliases_json, category, source, verified_flag, now, now),
            )
            conn.commit()
            return int(cur.lastrowid)

        existing_aliases = set()
        if row["aliases_json"]:
            try:
                existing_aliases = set(json.loads(row["aliases_json"]))
            except (ValueError, TypeError):
                existing_aliases = set()
        merged_aliases = sorted(existing_aliases | incoming_aliases)
        aliases_json = json.dumps(merged_aliases) if merged_aliases else None

        new_category = category or row["category"]
        new_verified = 1 if (verified_flag or int(row["verified"] or 0)) else 0

        source_priority = {
            "manual": 4,
            "seed": 3,
            "discovered_scraper": 2,
            "discovered_category": 1,
        }
        new_source = (
            source
            if source_priority.get(source, 0) >= source_priority.get(row["source"], 0)
            else row["source"]
        )

        conn.execute(
            """
            UPDATE brand_registry
            SET aliases_json = ?, category = ?, source = ?, verified = ?,
                last_seen_at = ?
            WHERE brand_id = ?
            """,
            (aliases_json, new_category, new_source, new_verified, now, row["brand_id"]),
        )
        conn.commit()
        return int(row["brand_id"])
    finally:
        conn.close()


def get_brand_from_registry(
    canonical_name: str, db_path: str | None = None
) -> dict[str, Any] | None:
    conn = _get_conn(db_path)
    try:
        row = conn.execute(
            "SELECT * FROM brand_registry WHERE LOWER(canonical_name) = LOWER(?)",
            ((canonical_name or "").strip(),),
        ).fetchone()
    finally:
        conn.close()
    if not row:
        return None
    d = dict(row)
    try:
        d["aliases"] = json.loads(d.get("aliases_json") or "[]")
    except (ValueError, TypeError):
        d["aliases"] = []
    return d


def list_all_brands_in_registry(db_path: str | None = None) -> list[dict[str, Any]]:
    conn = _get_conn(db_path)
    try:
        rows = conn.execute(
            "SELECT brand_id, canonical_name, aliases_json, category, source, "
            "times_queried, verified, added_at, last_seen_at "
            "FROM brand_registry ORDER BY canonical_name"
        ).fetchall()
    finally:
        conn.close()
    out = []
    for r in rows:
        d = dict(r)
        try:
            d["aliases"] = json.loads(d.get("aliases_json") or "[]")
        except (ValueError, TypeError):
            d["aliases"] = []
        out.append(d)
    return out


def increment_brand_queried(canonical_name: str, db_path: str | None = None) -> None:
    conn = _get_conn(db_path)
    try:
        conn.execute(
            """
            UPDATE brand_registry
            SET times_queried = times_queried + 1, last_seen_at = ?
            WHERE LOWER(canonical_name) = LOWER(?)
            """,
            (time.time(), (canonical_name or "").strip()),
        )
        conn.commit()
    finally:
        conn.close()


def count_new_brands_since(cutoff_ts: float, db_path: str | None = None) -> int:
    conn = _get_conn(db_path)
    try:
        row = conn.execute(
            "SELECT COUNT(*) AS n FROM brand_registry WHERE added_at > ?",
            (cutoff_ts,),
        ).fetchone()
    finally:
        conn.close()
    return int(row["n"]) if row else 0


def db_stats(db_path: str | None = None) -> dict[str, Any]:
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
            "brands_in_registry": _count("brand_registry"),
        }
    finally:
        conn.close()
