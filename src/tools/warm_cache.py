"""
Pre-warm the DB + Redis cache for an upcoming session.

Usage:
    python -m src.tools.warm_cache --brands "Dominos Pizza,McDonald's" --cities "Delhi,Mumbai"
    python -m src.tools.warm_cache --brands-file brands.txt --cities "Delhi"

Prints a cost estimate (based on Google Places pricing + current source
priority) and asks for confirmation before issuing any API calls.
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

from src.cache import db
from src.cache import manager as cache_manager
from src.cache.db import SOURCE_COST_USD
from src.config import logging_setup

logging_setup.configure()
logger = logging.getLogger("warm_cache")


EST_GOOGLE_CALLS_PER_QUERY = 3
EST_COST_PER_QUERY = EST_GOOGLE_CALLS_PER_QUERY * SOURCE_COST_USD["google_places"]


def _parse_list(value: str | None) -> list[str]:
    if not value:
        return []
    return [s.strip() for s in value.split(",") if s.strip()]


def _load_list_file(path: str | None) -> list[str]:
    if not path:
        return []
    return [
        line.strip()
        for line in Path(path).read_text().splitlines()
        if line.strip() and not line.startswith("#")
    ]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--brands", help="Comma-separated brand names")
    parser.add_argument("--brands-file", help="File of brand names (one per line)")
    parser.add_argument("--cities", help="Comma-separated city names", required=True)
    parser.add_argument(
        "--yes", action="store_true",
        help="Skip the cost confirmation prompt (for automated runs)",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Invalidate existing cached entries before fetching",
    )
    args = parser.parse_args(argv)

    brands = _parse_list(args.brands) + _load_list_file(args.brands_file)
    cities = _parse_list(args.cities)

    if not brands:
        parser.error("No brands supplied. Use --brands or --brands-file.")

    pairs = [(b, c) for b in brands for c in cities]
    est_cost = len(pairs) * EST_COST_PER_QUERY

    logger.info("Planning to warm cache for %d (brand, city) pairs:", len(pairs))
    for b, c in pairs:
        logger.info("  - %s / %s", b, c)
    logger.info(
        "Upper-bound cost estimate: $%.3f (assumes every pair hits Google Places "
        "for %d pages).",
        est_cost, EST_GOOGLE_CALLS_PER_QUERY,
    )

    if not args.yes:
        try:
            answer = input("Proceed? [y/N] ").strip().lower()
        except EOFError:
            answer = ""
        if answer not in {"y", "yes"}:
            logger.info("Aborted by user.")
            return 1

    try:
        from tqdm import tqdm  # type: ignore
        iterator = tqdm(pairs, desc="warming")
    except ImportError:
        iterator = pairs

    actual_cost_before = db.cumulative_api_cost()["total_usd"]

    for brand, city in iterator:
        if args.force:
            cache_manager.invalidate(brand, city)
        df, source = cache_manager.smart_fetch(brand, city)
        logger.info("  [%s] %s / %s -> %d rows", source, brand, city, len(df))

    actual_cost_after = db.cumulative_api_cost()["total_usd"]
    logger.info(
        "Done. Actual cost this run: $%.3f (cumulative $%.3f).",
        actual_cost_after - actual_cost_before,
        actual_cost_after,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
