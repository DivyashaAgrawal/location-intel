"""
Re-run `estimate_brand_size(..., force_refresh=True)` for every registered
brand. Intended for a monthly cron. Runs in roughly
(num_playwright_brands * 3s) + (num_full_scrape_brands * HTTP RTT).

Usage:
    python src/scripts/refresh_brand_sizes.py
"""
from __future__ import annotations

import logging
import sys
import time

from src.core.cache_manager import estimate_brand_size
from src.core.db import init_db
from src.fetchers.brand_scraper import BRAND_REGISTRY

logger = logging.getLogger(__name__)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    init_db()

    total_start = time.time()
    rows: list[tuple[str, str, object, float, int]] = []

    for brand in BRAND_REGISTRY:
        t0 = time.time()
        try:
            result = estimate_brand_size(brand, force_refresh=True)
        except Exception as e:
            logger.warning("%s: estimate failed: %s", brand, e)
            result = {"total_stores_estimate": None, "source": "error", "confidence": 0.0}
        dt_ms = int((time.time() - t0) * 1000)
        rows.append((
            brand,
            str(result.get("source")),
            result.get("total_stores_estimate"),
            float(result.get("confidence") or 0.0),
            dt_ms,
        ))
        logger.info(
            "%-18s source=%-22s estimate=%-8s conf=%.2f  (%d ms)",
            brand,
            result.get("source"),
            result.get("total_stores_estimate"),
            float(result.get("confidence") or 0.0),
            dt_ms,
        )

    total_s = time.time() - total_start
    logger.info("")
    logger.info("Refreshed %d brands in %.1f s", len(rows), total_s)
    return 0


if __name__ == "__main__":
    sys.exit(main())
