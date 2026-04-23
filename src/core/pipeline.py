from __future__ import annotations

import logging

import pandas as pd

from src.analysis.aggregator import (
    aggregate_stores,
    create_comparison_table,
    generate_executive_summary,
)
from src.analysis.competitor import run_competitor_analysis
from src.analysis.market_analysis import (
    compute_store_density,
    generate_ic_memo_points,
    peer_benchmark,
    whitespace_analysis,
)
from src.analysis.pincode_mapper import enrich_with_pincodes
from src.analysis.reconciler import reconcile, reconciliation_report
from src.analysis.sentiment import enrich_sentiment_from_ratings
from src.caching.cache_manager import (
    estimate_brand_size,
    estimate_enrichment_needed,
    get_already_enriched_cities,
    smart_fetch,
    smart_fetch_with_enrichment,
)
from src.caching.config import MAX_ENRICHMENT_CALLS_PER_QUERY, TIER_1_CITIES
from src.core.nlu import parse_query

logger = logging.getLogger(__name__)

REBUILD_INDEX_NAG_THRESHOLD = 20


def _register_discovered_brands_from_category(
    category_df: pd.DataFrame, category: str | None
) -> None:
    """
    After a category fetch, upsert every distinct brand name into the registry
    as an unverified discovered entry. Logs a reminder to rebuild the FAISS
    index once the backlog crosses REBUILD_INDEX_NAG_THRESHOLD.
    """
    if category_df is None or category_df.empty or "brand" not in category_df.columns:
        return
    try:
        from src.brand_resolver import INDEX_PATH
        from src.caching.db import count_new_brands_since, upsert_brand_to_registry
    except Exception as e:
        logger.debug(f"registry writeback skipped: {e}")
        return

    for raw_name in category_df["brand"].dropna().unique():
        name = str(raw_name).strip()
        if not name or len(name) <= 2:
            continue
        try:
            upsert_brand_to_registry(
                canonical_name=name,
                category=(category or None),
                source="discovered_category",
                verified=0,
            )
        except Exception as e:
            logger.debug(f"upsert_brand_to_registry({name!r}) failed: {e}")

    try:
        cutoff = INDEX_PATH.stat().st_mtime if INDEX_PATH.exists() else 0.0
        new_count = count_new_brands_since(cutoff)
    except Exception:
        return
    if new_count >= REBUILD_INDEX_NAG_THRESHOLD:
        logger.warning(
            f"{new_count} new brands discovered since last index rebuild. "
            f"Run `python src/scripts/rebuild_brand_index.py` to include them."
        )


def _mark_brand_verified_after_scrape(brand: str) -> None:
    """Hook for the brand scraper to flag a brand as verified in the registry."""
    if not brand:
        return
    try:
        from src.caching.db import upsert_brand_to_registry
        upsert_brand_to_registry(
            canonical_name=brand,
            source="discovered_scraper",
            verified=1,
        )
    except Exception as e:
        logger.debug(f"mark_brand_verified_after_scrape failed: {e}")


def run_pipeline(
    query: str,
    skip_geocoding: bool = False,
) -> dict:
    """
    Full pipeline from natural language query to output tables.

    Flow:
      1. NLU parse
      2. smart_fetch per (brand, city) through Redis -> DB -> APIs
      3. reconcile (if any raw records remain un-reconciled -- smart_fetch
         already reconciles when hitting the API tier)
      4. Pincode enrichment
      5. Sentiment enrichment
      6. Aggregation (pincode / city / state)
      7. Expansion / density analysis
      8. Competitor analysis (brand queries only)

    Returns a dict with these keys:
        parsed_query, raw_stores, summary_table, executive_summary,
        comparison_table, density_tables, whitespace_tables, ic_memo_points,
        peer_benchmark, reconciliation_report, competitor_analysis,
        fetch_sources.
    """
    parsed = parse_query(query)
    logger.info(f"Parsed query: {parsed}")

    brands = parsed.get("brands", [])
    cities = parsed.get("geography", {}).get("filter", [])
    geo_level = parsed.get("geography", {}).get("level", "city")
    is_comparison = parsed.get("comparison", False)
    query_type = parsed.get("query_type", "brand")
    category = parsed.get("category")
    search_query = parsed.get("search_query")

    empty_result = {
        "parsed_query": parsed,
        "raw_stores": pd.DataFrame(),
        "summary_table": pd.DataFrame(),
        "executive_summary": "Could not identify any brand in the query.",
        "comparison_table": None,
        "density_tables": {},
        "whitespace_tables": {},
        "ic_memo_points": {},
        "peer_benchmark": None,
        "reconciliation_report": {"status": "no data"},
        "competitor_analysis": None,
        "fetch_sources": {},
        "brand_sizes": {},
        "enrichment_metadata": {},
    }

    if query_type == "brand" and not brands:
        return empty_result

    enrichment_metadata_by_brand: dict[str, dict] = {}
    brand_sizes: dict[str, dict] = {}
    if query_type == "brand":
        for brand in brands:
            try:
                size = estimate_brand_size(brand)
            except Exception as e:
                logger.warning(f"brand-size estimate failed for {brand}: {e}")
                size = {"brand": brand, "total_stores_estimate": None, "confidence": 0.0}
            brand_sizes[brand] = size
            est = size.get("total_stores_estimate")
            logger.info(
                f"Brand size for {brand}: "
                f"~{est if est is not None else 'unknown'} stores "
                f"(source={size.get('source')}, "
                f"confidence={size.get('confidence')})"
            )

    if query_type == "brand":
        over_budget: list[dict] = []
        for brand in brands:
            projected = estimate_enrichment_needed(brand, cities)
            if projected > MAX_ENRICHMENT_CALLS_PER_QUERY:
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
                            f"Start with one city, e.g., "
                            f"'{brand} in {TIER_1_CITIES[0]}'"
                        ),
                    },
                })

        if over_budget:
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

    raw_records_by_brand: dict[str, pd.DataFrame] = {}
    fetch_sources: dict[tuple[str, str], str] = {}
    combined_raw_list: list[pd.DataFrame] = []

    if query_type == "category":
        logger.info("[2/8] Category fetch via smart_fetch...")
        # For category queries we use a single marker brand label ("__category__")
        # in the smart_fetch call so the DB's query_cache can still key it;
        # the actual brand names come from the returned records.
        category_label = search_query or category or "category"
        category_frames = []
        for city in cities:
            df, source = smart_fetch(category_label, city)
            fetch_sources[(category_label, city)] = source
            if df is not None and not df.empty:
                category_frames.append(df)

        if category_frames:
            raw_category = pd.concat(category_frames, ignore_index=True, sort=False)
            if "brand" in raw_category.columns:
                for brand_name in raw_category["brand"].dropna().unique():
                    raw_records_by_brand[brand_name] = raw_category[
                        raw_category["brand"] == brand_name
                    ].copy()
                # Phase 5.4: feed discovered brands into brand_registry so the
                # resolver learns about them on next index rebuild.
                _register_discovered_brands_from_category(raw_category, category)
            brands = list(raw_records_by_brand.keys())
            combined_raw_list.append(raw_category)

        logger.info(
            f"Category '{category}': {sum(len(df) for df in raw_records_by_brand.values())} "
            f"records across {len(brands)} brands"
        )

    else:
        logger.info("[2/8] Brand fetch via smart_fetch_with_enrichment...")
        for brand in brands:
            try:
                brand_df, fetch_meta = smart_fetch_with_enrichment(brand, cities)
            except Exception as e:
                logger.warning(f"smart_fetch_with_enrichment failed for {brand}: {e}")
                brand_df, fetch_meta = pd.DataFrame(), {"error": str(e)}

            # Fallback: if the two-stage path yielded nothing (e.g. brand not in
            # scraper registry and no Places key), fall back to per-city
            # smart_fetch so emergency sources (Serper/OSM/mock) still run.
            if brand_df is None or brand_df.empty:
                frames = []
                for city in cities:
                    df, source = smart_fetch(brand, city)
                    fetch_sources[(brand, city)] = source
                    if df is not None and not df.empty:
                        frames.append(df)
                brand_df = (
                    pd.concat(frames, ignore_index=True, sort=False)
                    if frames else pd.DataFrame()
                )
            else:
                for city in cities:
                    fetch_sources[(brand, city)] = (
                        "enrichment" if fetch_meta.get("stores_enriched_this_call") else "db"
                    )

            if not brand_df.empty:
                brand_df["brand"] = brand

            enrichment_metadata_by_brand[brand] = fetch_meta
            raw_records_by_brand[brand] = brand_df
            combined_raw_list.append(brand_df)
            logger.info(
                f"{brand}: {len(brand_df)} records "
                f"(stage1_records={fetch_meta.get('stage1_records', 0)}, "
                f"enriched={fetch_meta.get('stores_enriched_this_call', 0)})"
            )

    combined_raw = (
        pd.concat([df for df in combined_raw_list if df is not None and not df.empty],
                  ignore_index=True, sort=False)
        if combined_raw_list
        else pd.DataFrame()
    )

    logger.info("[3/8] Reconciling...")
    if combined_raw.empty:
        merged = pd.DataFrame()
        recon_report: dict = {"status": "no data"}
    else:
        # reconcile() is idempotent: it no-ops on already-reconciled input
        # (no raw `source` column) and clusters across sources otherwise.
        try:
            merged = reconcile(combined_raw)
        except Exception as e:
            logger.warning(f"reconcile failed: {e}; using raw records")
            merged = combined_raw
        recon_report = reconciliation_report(combined_raw, merged)
        logger.info(
            f"{len(combined_raw)} raw -> {len(merged)} unique stores "
            f"({recon_report.get('dedup_ratio', recon_report.get('status', ''))})"
        )

    all_stores = merged

    logger.info("[4/8] Pincode enrichment...")
    if not skip_geocoding and not all_stores.empty:
        if "pincode" not in all_stores.columns or all_stores["pincode"].isna().all():
            logger.info("Reverse geocoding (~1 sec per store)...")
            all_stores = enrich_with_pincodes(all_stores)
    else:
        logger.info("Skipping geocoding")

    logger.info("[5/8] Sentiment...")
    all_stores = enrich_sentiment_from_ratings(all_stores)

    logger.info("[6/8] Aggregating...")
    brand_dfs: dict[str, pd.DataFrame] = {}
    if not all_stores.empty and "brand" in all_stores.columns:
        for brand in brands:
            brand_dfs[brand] = all_stores[all_stores["brand"] == brand].copy()
    else:
        for brand in brands:
            brand_dfs[brand] = pd.DataFrame()

    if is_comparison and len(brands) > 1:
        summary_table = create_comparison_table(brand_dfs, group_level=geo_level)
        comparison_table = summary_table
    else:
        summary_table = aggregate_stores(all_stores, group_level=geo_level)
        comparison_table = None

    exec_summaries = [generate_executive_summary(brand_dfs[b], b) for b in brands]
    executive_summary = "\n\n".join(exec_summaries)

    logger.info("[7/8] Expansion analysis...")
    density_tables: dict[str, pd.DataFrame] = {}
    whitespace_tables: dict[str, pd.DataFrame] = {}
    ic_memo_points: dict[str, list[str]] = {}

    for brand in brands:
        bdf = brand_dfs[brand]
        if not bdf.empty:
            density = compute_store_density(bdf)
            ws = whitespace_analysis(bdf)
            memo = generate_ic_memo_points(brand, density, ws)
            density_tables[brand] = density
            whitespace_tables[brand] = ws
            ic_memo_points[brand] = memo

    benchmark = peer_benchmark(brand_dfs) if len(brands) > 1 else None

    logger.info("[8/8] Competitor analysis...")
    competitor_analysis = None
    # Only run competitor analysis for single-brand deep dives (not
    # comparisons or category queries -- those already surface alternates).
    if query_type == "brand" and not is_comparison and len(brands) == 1:
        focal = brands[0]
        focal_df = brand_dfs.get(focal, pd.DataFrame())
        if not focal_df.empty:
            try:
                competitor_analysis = run_competitor_analysis(
                    focal_brand=focal,
                    focal_stores=focal_df,
                    cities=cities,
                    fetch_fn=smart_fetch,
                )
            except Exception as e:
                logger.warning(f"competitor analysis failed: {e}")
                competitor_analysis = None

    logger.info("Pipeline complete.")

    return {
        "parsed_query": parsed,
        "raw_stores": all_stores,
        "summary_table": summary_table,
        "executive_summary": executive_summary,
        "comparison_table": comparison_table,
        "density_tables": density_tables,
        "whitespace_tables": whitespace_tables,
        "ic_memo_points": ic_memo_points,
        "peer_benchmark": benchmark,
        "reconciliation_report": recon_report,
        "competitor_analysis": competitor_analysis,
        "fetch_sources": {f"{b}|{c}": s for (b, c), s in fetch_sources.items()},
        "brand_sizes": brand_sizes,
        "enrichment_metadata": enrichment_metadata_by_brand,
    }
