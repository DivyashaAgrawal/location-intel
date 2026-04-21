from __future__ import annotations

import logging

import pandas as pd
from src.nlu import parse_query
from src.core.cache_manager import smart_fetch
from src.analysis.reconciler import reconcile, reconciliation_report
from src.analysis.competitor import run_competitor_analysis
from src.analysis.pincode_mapper import enrich_with_pincodes
from src.analysis.sentiment import enrich_sentiment_from_ratings
from src.analysis.aggregator import aggregate_stores, create_comparison_table, generate_executive_summary
from src.analysis.market_analysis import compute_store_density, whitespace_analysis, peer_benchmark, generate_ic_memo_points

logger = logging.getLogger(__name__)


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
    }

    if query_type == "brand" and not brands:
        return empty_result

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
            brands = list(raw_records_by_brand.keys())
            combined_raw_list.append(raw_category)

        logger.info(
            f"Category '{category}': {sum(len(df) for df in raw_records_by_brand.values())} "
            f"records across {len(brands)} brands"
        )

    else:
        logger.info("[2/8] Brand fetch via smart_fetch...")
        for brand in brands:
            frames = []
            for city in cities:
                df, source = smart_fetch(brand, city)
                fetch_sources[(brand, city)] = source
                if df is not None and not df.empty:
                    frames.append(df)
            brand_df = pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()
            if not brand_df.empty:
                brand_df["brand"] = brand
            raw_records_by_brand[brand] = brand_df
            combined_raw_list.append(brand_df)
            sources_summary = ", ".join(
                f"{c}={fetch_sources.get((brand, c), '?')}" for c in cities
            )
            logger.info(f"{brand}: {len(brand_df)} records [{sources_summary}]")

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
    }
