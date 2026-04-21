"""
Dump the persistent store DB for external BI tooling.

Usage:
    python -m src.tools.export_data --format csv  --output /tmp/stores.csv
    python -m src.tools.export_data --format xlsx --output /tmp/stores.xlsx --brand "Dominos Pizza"
    python -m src.tools.export_data --format json --output /tmp/stores.json
"""
from __future__ import annotations

import argparse
import logging
import sqlite3
from pathlib import Path

import pandas as pd

from src import logging_setup
from src.core import db as _db

logging_setup.configure()
logger = logging.getLogger("export_data")


def _load_stores(brand: str | None = None, city: str | None = None) -> pd.DataFrame:
    """Load `stores` joined with the latest rating snapshot."""
    conn = sqlite3.connect(_db.DEFAULT_DB_PATH)
    conn.row_factory = sqlite3.Row
    query = """
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
    """
    clauses = []
    params: list[object] = []
    if brand:
        clauses.append("LOWER(s.brand) = LOWER(?)")
        params.append(brand)
    if city:
        clauses.append("LOWER(s.city) = LOWER(?)")
        params.append(city)
    if clauses:
        query += " WHERE " + " AND ".join(clauses)

    rows = conn.execute(query, params).fetchall()
    conn.close()
    return pd.DataFrame([dict(r) for r in rows])


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--format", choices=["csv", "json", "xlsx"], default="csv",
    )
    parser.add_argument("--output", required=True, help="Output file path")
    parser.add_argument("--brand", help="Filter by brand (exact, case-insensitive)")
    parser.add_argument("--city", help="Filter by city (exact, case-insensitive)")
    args = parser.parse_args(argv)

    df = _load_stores(brand=args.brand, city=args.city)
    if df.empty:
        logger.warning("No stores in the DB matched the filters.")
        return 2

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)

    if args.format == "csv":
        df.to_csv(out, index=False)
    elif args.format == "json":
        df.to_json(out, orient="records", indent=2, date_format="iso")
    elif args.format == "xlsx":
        with pd.ExcelWriter(out, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="stores", index=False)

    logger.info("Wrote %d rows to %s", len(df), out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
