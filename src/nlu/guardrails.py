from __future__ import annotations

import logging

from src.config.settings import MAX_ENRICHMENT_CALLS_PER_QUERY, TIER_1_CITIES
from src.nlu.brand_size import estimate_enrichment_needed, get_already_enriched_cities

logger = logging.getLogger(__name__)


def check_query_budget(
    brands: list[str],
    cities: list[str],
    brand_sizes: dict[str, dict],
) -> list[dict]:
    """
    Project enrichment cost per brand and flag any that would blow past
    MAX_ENRICHMENT_CALLS_PER_QUERY. Returns a list of "scope" dicts suitable
    for embedding in a blocked-query response. Empty list == all clear.
    """
    over_budget: list[dict] = []
    for brand in brands:
        projected = estimate_enrichment_needed(brand, cities)
        if projected <= MAX_ENRICHMENT_CALLS_PER_QUERY:
            continue
        size = brand_sizes.get(brand, {})
        over_budget.append({
            "brand": brand,
            "total_brand_size": size.get("total_stores_estimate"),
            "cities_requested": cities or ["all India"],
            "projected_api_calls": projected,
            "threshold": MAX_ENRICHMENT_CALLS_PER_QUERY,
            "already_enriched_cities": get_already_enriched_cities(brand),
            "tier_1_cities_available": TIER_1_CITIES,
            "suggestion": {
                "try_instead": (
                    f"Start with one city, e.g., '{brand} in {TIER_1_CITIES[0]}'"
                ),
            },
        })
    return over_budget


def build_blocked_response(parsed: dict, over_budget: list[dict], brand_sizes: dict[str, dict]) -> dict:
    """Shape the full pipeline response dict used when a query is blocked."""
    import pandas as pd
    first = over_budget[0]
    return {
        "parsed_query": parsed,
        "status": "blocked",
        "reason": "query_too_large",
        "message": (
            "This query is too large to run in a single pass. "
            "For now, please query city by city. As more queries "
            "run, the database will fill up and subsequent queries "
            "will be faster and cheaper."
        ),
        "scope": first,
        "scopes": over_budget,
        "raw_stores": pd.DataFrame(),
        "summary_table": pd.DataFrame(),
        "executive_summary": "Query blocked: projected enrichment too large.",
        "comparison_table": None,
        "density_tables": {},
        "whitespace_tables": {},
        "ic_memo_points": {},
        "peer_benchmark": None,
        "reconciliation_report": {"status": "blocked"},
        "competitor_analysis": None,
        "fetch_sources": {},
        "brand_sizes": brand_sizes,
        "enrichment_metadata": {},
    }
