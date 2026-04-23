import io

import pandas as pd
import streamlit as st

from src.caching import cache_manager
from src.core.pipeline import run_pipeline


def _render_blocked(result: dict) -> None:
    scope = result.get("scope") or {}
    brand = scope.get("brand") or "Brand"
    total = scope.get("total_brand_size")
    projected = scope.get("projected_api_calls") or 0

    st.warning(result.get("message") or "This query is too large.")

    lines = []
    if total:
        lines.append(
            f"**{brand}** has ~{total:,} stores across India. Running this "
            f"in one pass would make ~{projected} enrichment call(s)."
        )
    else:
        lines.append(
            f"Running **{brand}** across the requested scope would make "
            f"~{projected} enrichment call(s) - above the threshold."
        )
    st.markdown("\n".join(lines))

    already = scope.get("already_enriched_cities") or []
    tier1 = scope.get("tier_1_cities_available") or []

    if tier1:
        st.markdown("**Try instead (tier-1 cities):**")
        cols = st.columns(len(tier1))
        for col, city in zip(cols, tier1, strict=False):
            q = f"{brand} in {city}"
            if col.button(q, key=f"blocked-suggest-{brand}-{city}"):
                st.session_state["query_input"] = q
                st.rerun()

    if already:
        st.markdown("**Cities already in our DB:**")
        cols = st.columns(min(len(already), 4) or 1)
        for i, row in enumerate(already):
            col = cols[i % len(cols)]
            city = row["city"]
            label = f"{city} ({row['store_count']} stores)"
            q = f"{brand} in {city}"
            if col.button(label, key=f"blocked-db-{brand}-{city}"):
                st.session_state["query_input"] = q
                st.rerun()


def render() -> None:
    stats = cache_manager.cache_stats()

    st.set_page_config(page_title="Location Intelligence", layout="wide")

    st.markdown(
        """
        <style>
            .block-container { padding-top: 2rem; }
            div[data-testid="stMetric"] { background: #f8f9fa; border-radius: 8px; padding: 12px 16px; }
            .territory-defensible { color: #0B7A4A; font-weight: 600; }
            .territory-contested  { color: #B76E00; font-weight: 600; }
            .territory-whitespace { color: #B03A2E; font-weight: 600; }
            .territory-open       { color: #555;    font-weight: 500; }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.title("Location Intelligence")
    st.caption("Retail brand location Intelligence.")

    with st.sidebar:
        st.header("Configuration")

        sources = stats["sources"]
        if sources["google_places"]:
            st.success("Google Places: connected")
        else:
            st.warning("GOOGLE_PLACES_API_KEY missing (primary maps source)")

        if sources["serper"]:
            st.success("Serper: connected (fallback)")
        else:
            st.info("SERPER_API_KEY missing (optional fallback)")

        st.divider()
        with st.expander("Cache & cost", expanded=False):
            st.write(
                f"**Redis**: {'up' if stats['redis_available'] else 'down (using SQLite)'}"
            )
            db_stats = stats["db"]
            st.write(f"**Stores in DB**: {db_stats['stores']}")
            st.write(f"**Rating snapshots**: {db_stats['store_ratings']}")
            st.write(f"**Cached queries**: {db_stats['query_cache_entries']}")
            cost = stats["api_cost"]
            st.write(
                f"**Total API calls**: {cost['total_calls']} "
                f"(~${cost['total_usd']} USD cumulative)"
            )
            if cost["by_source"]:
                st.write("**By source**")
                st.dataframe(
                    pd.DataFrame(cost["by_source"]),
                    width="stretch",
                    hide_index=True,
                )

        st.divider()
        with st.expander("Discovered competitors", expanded=False):
            from src.caching import db as _db
            rows = _db.list_all_discovered_competitors()
            if not rows:
                st.caption(
                    "None yet. Category queries like 'all pizza stores in Delhi' "
                    "will populate this list over time."
                )
            else:
                for row in rows[:20]:
                    brand = row["brand"]
                    verified = bool(row.get("manually_verified"))
                    label = (
                        f"**{brand}** ({row['category']}, seen {row['times_seen']}x)"
                        f"{' verified' if verified else ''}"
                    )
                    st.markdown(label)
                    c_ok, c_no = st.columns(2)
                    if not verified:
                        if c_ok.button("Confirm", key=f"verify-{brand}"):
                            _db.verify_discovered_competitor(brand)
                            st.rerun()
                    if c_no.button("Flag as noise", key=f"delete-{brand}"):
                        _db.delete_discovered_competitor(brand)
                        st.rerun()

        st.divider()
        st.subheader("Example queries")
        examples = [
            "pincode wise Dominos stores in Delhi and Mumbai with ratings",
            "compare Haldirams vs McDonald's in Delhi, Mumbai, Bangalore",
            "state wise Da Milano locations with sentiment",
            "city wise Dominos vs Pizzahut vs la pinoz across all metros",
        ]
        for ex in examples:
            if st.button(f"-> {ex[:55]}...", key=ex, width="stretch"):
                st.session_state["query_input"] = ex

    query = st.text_area(
        "What do you want to know?",
        value=st.session_state.get("query_input", ""),
        placeholder="e.g., get me pincode wise store summary of Dominos in Delhi",
        height=72,
    )

    run_button = st.button("Analyze", type="primary")

    if run_button and query:
        with st.spinner("Running location intelligence pipeline..."):
            result = run_pipeline(query)

        if result.get("status") == "blocked":
            _render_blocked(result)
            st.stop()

        raw = result["raw_stores"]

        if raw is None or raw.empty:
            st.error("No data found. Try rephrasing your query or check the brand name.")
            st.json(result["parsed_query"])
            st.stop()

        with st.expander("Parsed query", expanded=False):
            st.json(result["parsed_query"])

        with st.expander("Data Quality & Sources", expanded=False):
            st.json(result.get("reconciliation_report", {}))

        enrich_meta = result.get("enrichment_metadata") or {}
        if enrich_meta:
            with st.expander("Data sources for this query", expanded=False):
                stats = cache_manager.cache_stats()
                cost_total = stats.get("api_cost", {}).get("total_usd", 0.0)
                for brand, meta in enrich_meta.items():
                    lines = [f"**{brand}**"]
                    if meta.get("stage1_ran"):
                        lines.append(
                            f"- Brand website scrape: {meta.get('stage1_records', 0)} "
                            "stores nationally (fresh)"
                        )
                    else:
                        lines.append(
                            "- Brand website scrape: cached national footprint"
                        )
                    enriched = meta.get("stores_enriched_this_call", 0)
                    if enriched:
                        lines.append(
                            f"- Google Places enrichment: {enriched} store(s) "
                            f"in {', '.join(meta.get('queried_cities', []))} (just refreshed)"
                        )
                    from_cache = meta.get("stores_from_cache", 0)
                    if from_cache:
                        lines.append(
                            f"- DB hit: {from_cache} store(s) already enriched"
                        )
                    errs = meta.get("stage2_errors") or []
                    if errs:
                        lines.append(f"- Enrichment errors: {len(errs)}")
                    st.markdown("\n".join(lines))
                st.caption(f"Cumulative API cost across all queries: ${cost_total:.4f} USD")

        with st.expander("Fetch source per (brand, city)", expanded=False):
            fs = result.get("fetch_sources", {})
            if fs:
                rows = [{"query": k, "answered_by": v} for k, v in fs.items()]
                st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)
                counts = pd.Series(list(fs.values())).value_counts().to_dict()
                st.caption(f"Source mix: {counts}")
            else:
                st.caption("No fetches recorded this run.")

        brand_sizes = result.get("brand_sizes") or {}
        if brand_sizes:
            for brand, size in brand_sizes.items():
                total = size.get("total_stores_estimate")
                coverage = size.get("coverage_pct")
                source = size.get("source") or "unknown"
                cities_queried = result["parsed_query"].get("geography", {}).get("filter", [])
                brand_rows = raw[raw["brand"] == brand] if "brand" in raw.columns else raw.iloc[0:0]
                if total is None:
                    st.caption(
                        f"**{brand}** - total size not available (source: {source}). "
                        f"Currently showing {len(brand_rows)} stores in "
                        f"{', '.join(cities_queried) or 'query'}."
                    )
                else:
                    pieces = [
                        f"**{brand}** (~{total:,} stores in India, source: {source})",
                        f"Currently showing: {len(brand_rows)} stores in "
                        f"{', '.join(cities_queried) or 'query'}",
                    ]
                    if coverage is not None:
                        enriched_count = int(round(coverage / 100 * total))
                        pieces.append(
                            f"DB coverage: {enriched_count} of {total:,} stores "
                            f"enriched ({coverage}%)"
                        )
                    st.caption(" \u00b7 ".join(pieces))

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Stores", len(raw))
        c2.metric("Brands", raw["brand"].nunique() if "brand" in raw.columns else 0)
        c3.metric("Cities", raw["city"].nunique() if "city" in raw.columns else 0)
        c4.metric(
            "Avg Rating",
            f"{raw['rating'].mean():.1f}" if "rating" in raw.columns and raw["rating"].notna().any() else "N/A",
        )
        c5.metric(
            "Reviews",
            f"{int(raw['review_count'].sum()):,}" if "review_count" in raw.columns and raw["review_count"].notna().any() else "0",
        )

        st.info(result["executive_summary"])

        tab_summary, tab_competitor, tab_expansion, tab_peer, tab_raw = st.tabs([
            "Summary Table",
            "Competitor Analysis",
            "Expansion Analysis",
            "Peer Benchmark",
            "Raw Store Data",
        ])

        with tab_summary:
            summary = result["summary_table"]
            if summary is None or summary.empty:
                st.info("No rows to summarise.")
            else:
                total_rows = len(summary)
                page_options = ["25", "50", "100", "All"]
                default_idx = page_options.index("50")
                col_left, _ = st.columns([1, 3])
                with col_left:
                    rows_choice = st.selectbox(
                        "Rows per page",
                        page_options,
                        index=default_idx,
                        key="summary_rows_per_page",
                    )
                display_df = summary if rows_choice == "All" else summary.head(int(rows_choice))

                st.caption(f"Showing {len(display_df)} of {total_rows} rows")
                st.dataframe(
                    display_df, width="stretch", hide_index=True,
                    column_config={
                        "avg_rating": st.column_config.NumberColumn("Avg Rating", format="%.1f"),
                        "total_reviews": st.column_config.NumberColumn("Total Reviews", format="%d"),
                        "store_count": st.column_config.NumberColumn("# Stores", format="%d"),
                        "positive_feedback_%": st.column_config.ProgressColumn(
                            "Positive %", min_value=0, max_value=100, format="%.1f%%",
                        ),
                    },
                )
                csv = summary.to_csv(index=False)
                st.download_button(
                    f"Download CSV (all {total_rows} rows)",
                    data=csv,
                    file_name="location_summary.csv",
                    mime="text/csv",
                )

            if result.get("comparison_table") is not None:
                st.subheader("Brand Comparison")
                st.dataframe(result["comparison_table"], width="stretch", hide_index=True)

        with tab_competitor:
            comp = result.get("competitor_analysis")
            if comp is None:
                st.info(
                    "Competitor analysis runs only for single-brand deep dives. "
                    "Try a query like 'pincode wise Dominos in Delhi'."
                )
            else:
                st.subheader(f"Focal brand: {result['parsed_query']['brands'][0]}")

                comps = comp.get("competitors") or []
                if comps:
                    st.caption(f"Auto-identified competitors: {', '.join(comps)}")
                else:
                    st.caption("No direct competitors found in the competitor map.")

                memo = comp.get("memo_points") or []
                if memo:
                    st.markdown("**IC memo bullets:**")
                    for m in memo:
                        st.markdown(f"- {m}")
                    st.divider()

                sov = comp.get("share_of_voice")
                if sov is not None and not sov.empty:
                    st.markdown("**Share of voice (stores in the mapped set)**")
                    display_sov = sov.copy()
                    display_sov["brand"] = display_sov.apply(
                        lambda r: f"{r['brand']} (focal)" if r["is_focal_brand"] else r["brand"],
                        axis=1,
                    )
                    st.dataframe(
                        display_sov.drop(columns=["is_focal_brand"]),
                        width="stretch", hide_index=True,
                        column_config={
                            "share_of_voice_%": st.column_config.ProgressColumn(
                                "Share of Voice", min_value=0, max_value=100, format="%.1f%%",
                            ),
                        },
                    )
                    chart = sov[["brand", "store_count"]].set_index("brand")
                    st.bar_chart(chart, color="#534AB7")

                terr = comp.get("territory_by_pincode")
                if terr is not None and not terr.empty:
                    st.markdown("**Territory classification (per pincode)**")

                    def _colour(row):
                        t = row["territory"]
                        colour = {
                            "Defensible territory": "#e6f6ef",
                            "Contested": "#fdf3e3",
                            "Competitor whitespace": "#fbe7e4",
                            "Open market": "#f4f4f4",
                        }.get(t, "white")
                        return [f"background-color: {colour}"] * len(row)

                    st.dataframe(
                        terr.style.apply(_colour, axis=1),
                        width="stretch", hide_index=True,
                    )

                    counts = terr["territory"].value_counts().to_dict()
                    cols = st.columns(4)
                    cols[0].metric("Defensible", counts.get("Defensible territory", 0))
                    cols[1].metric("Contested", counts.get("Contested", 0))
                    cols[2].metric("Competitor whitespace", counts.get("Competitor whitespace", 0))
                    cols[3].metric("Open market", counts.get("Open market", 0))

        with tab_expansion:
            for brand, ws in result.get("whitespace_tables", {}).items():
                st.subheader(f"{brand} - Expansion Opportunities")

                points = result.get("ic_memo_points", {}).get(brand, [])
                if points:
                    st.markdown("**IC Memo Points:**")
                    for p in points:
                        st.markdown(f"- {p}")
                st.divider()

                density = result.get("density_tables", {}).get(brand)
                if density is not None and not density.empty:
                    st.markdown("**Store Density (stores per 100K population)**")
                    st.dataframe(
                        density[["city", "city_tier", "store_count", "population", "stores_per_100k", "avg_rating"]],
                        width="stretch", hide_index=True,
                        column_config={
                            "stores_per_100k": st.column_config.NumberColumn("Per 100K", format="%.2f"),
                            "population": st.column_config.NumberColumn("Population", format="%d"),
                        },
                    )

                if ws is not None and not ws.empty:
                    st.markdown("**Whitespace / Expansion Scoring**")
                    st.dataframe(
                        ws[["city", "city_tier", "population", "current_stores", "stores_per_100k",
                            "opportunity", "expansion_score"]],
                        width="stretch", hide_index=True,
                        column_config={
                            "expansion_score": st.column_config.ProgressColumn(
                                "Score", min_value=0, max_value=100, format="%.0f",
                            ),
                            "population": st.column_config.NumberColumn("Population", format="%d"),
                        },
                    )
                    chart_data = ws[["city", "expansion_score"]].set_index("city").sort_values(
                        "expansion_score", ascending=True
                    )
                    st.bar_chart(chart_data, horizontal=True, color="#1D9E75")

        with tab_peer:
            benchmark = result.get("peer_benchmark")
            if benchmark is not None and not benchmark.empty:
                st.subheader("Brand Comparison Matrix")
                st.dataframe(
                    benchmark, width="stretch", hide_index=True,
                    column_config={
                        "tier1_coverage_%": st.column_config.ProgressColumn(
                            "Tier 1 Coverage %", min_value=0, max_value=100, format="%.0f%%",
                        ),
                        "population_coverage_%": st.column_config.ProgressColumn(
                            "Pop Coverage %", min_value=0, max_value=100, format="%.1f%%",
                        ),
                    },
                )
                col_a, col_b = st.columns(2)
                with col_a:
                    st.markdown("**Store count by brand**")
                    st.bar_chart(benchmark[["brand", "total_stores"]].set_index("brand"), color="#534AB7")
                with col_b:
                    st.markdown("**Avg rating by brand**")
                    st.bar_chart(benchmark[["brand", "avg_rating"]].set_index("brand"), color="#D85A30")
            else:
                st.info("Run a multi-brand query (e.g., 'compare Dominos vs McDonald\\'s') to see peer benchmarks.")

        with tab_raw:
            display_cols = [
                c for c in ["brand", "title", "address", "city", "state", "pincode",
                            "rating", "review_count", "positive_pct", "negative_pct",
                            "sources", "source_count", "data_quality"]
                if c in raw.columns
            ]
            st.dataframe(raw[display_cols], width="stretch", hide_index=True)

            st.subheader("Export")
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine="openpyxl") as writer:
                if result["summary_table"] is not None and not result["summary_table"].empty:
                    result["summary_table"].to_excel(writer, sheet_name="Summary", index=False)
                raw[display_cols].to_excel(writer, sheet_name="Store Details", index=False)
                for brand, ws in result.get("whitespace_tables", {}).items():
                    if ws is not None and not ws.empty:
                        ws.to_excel(writer, sheet_name=f"Expansion-{brand[:28]}", index=False)
                bm = result.get("peer_benchmark")
                if bm is not None and not bm.empty:
                    bm.to_excel(writer, sheet_name="Peer Benchmark", index=False)
                comp = result.get("competitor_analysis")
                if comp:
                    terr = comp.get("territory_by_pincode")
                    if terr is not None and not terr.empty:
                        terr.to_excel(writer, sheet_name="Territory", index=False)
                    sov = comp.get("share_of_voice")
                    if sov is not None and not sov.empty:
                        sov.to_excel(writer, sheet_name="ShareOfVoice", index=False)

            st.download_button(
                "Download Full Report (Excel)",
                data=buf.getvalue(),
                file_name="location_intelligence_report.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

    elif run_button:
        st.warning("Please enter a query.")

    st.divider()
    st.caption(
        "Location Intelligence | Data: Brand websites + Google Places + Serper + OSM | "
        "NLU: Ollama + rule-based"
    )

if __name__ == "__main__":
    render()
