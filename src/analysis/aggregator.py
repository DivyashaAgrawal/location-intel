"""
Aggregation module: Transform raw store data into the final summary tables.

Supports aggregation at: pincode, city, state, district, national level.
"""
from __future__ import annotations

import logging

import pandas as pd

logger = logging.getLogger(__name__)

_PRIMARY_KEY_BY_LEVEL = {
    "pincode": "pincode",
    "city": "city",
    "state": "state",
    "district": "district",
}


def aggregate_stores(
    df: pd.DataFrame,
    group_level: str = "pincode",
    brand_col: str = "brand",
) -> pd.DataFrame:
    """
    Aggregate store data at the specified geography level.

    Args:
        df: Raw store-level DataFrame
        group_level: One of "pincode", "city", "state", "district", "national"
        brand_col: Column name for brand

    Returns:
        Aggregated DataFrame with summary metrics per geography unit.
    """
    if df.empty:
        return pd.DataFrame()

    df = df.copy()

    group_cols = [brand_col]

    if group_level == "pincode":
        group_cols += ["pincode", "city", "state"]
    elif group_level == "city":
        group_cols += ["city", "state"]
    elif group_level == "state":
        group_cols += ["state"]
    elif group_level == "district":
        group_cols += ["district", "state"]

    # Only drop rows missing brand or the level-specific primary key. Secondary
    # keys (city, state on a pincode rollup) are preserved as NaN groups so
    # partial-data stores still appear in the output.
    required_cols = [brand_col]
    primary_key = _PRIMARY_KEY_BY_LEVEL.get(group_level)
    if primary_key and primary_key != brand_col:
        required_cols.append(primary_key)

    before = len(df)
    for col in required_cols:
        if col in df.columns:
            df = df[df[col].notna() & (df[col] != "")]
    dropped = before - len(df)
    if dropped:
        logger.warning(
            "aggregate_stores: dropped %d row(s) missing %s for %s-level rollup",
            dropped,
            "/".join(required_cols),
            group_level,
        )

    available_group_cols = [c for c in group_cols if c in df.columns]

    if not available_group_cols:
        available_group_cols = [brand_col]

    agg_dict = {}

    if "title" in df.columns:
        agg_dict["title"] = "count"
    elif "address" in df.columns:
        agg_dict["address"] = "count"

    if "rating" in df.columns:
        agg_dict["rating"] = "mean"

    if "review_count" in df.columns:
        agg_dict["review_count"] = "sum"

    if "positive_pct" in df.columns:
        agg_dict["positive_pct"] = "mean"
    if "negative_pct" in df.columns:
        agg_dict["negative_pct"] = "mean"
    if "neutral_pct" in df.columns:
        agg_dict["neutral_pct"] = "mean"

    if not agg_dict:
        return pd.DataFrame()

    result = df.groupby(available_group_cols, as_index=False, dropna=False).agg(agg_dict)

    rename_map = {
        "title": "store_count",
        "address": "store_count",
        "rating": "avg_rating",
        "review_count": "total_reviews",
        "positive_pct": "positive_feedback_%",
        "negative_pct": "negative_feedback_%",
        "neutral_pct": "neutral_feedback_%",
    }
    result = result.rename(columns={k: v for k, v in rename_map.items() if k in result.columns})

    numeric_cols = result.select_dtypes(include="number").columns
    result[numeric_cols] = result[numeric_cols].round(1)

    if "store_count" in result.columns:
        result = result.sort_values("store_count", ascending=False)

    return result.reset_index(drop=True)


def create_comparison_table(
    dfs: dict[str, pd.DataFrame],
    group_level: str = "city",
) -> pd.DataFrame:
    """
    Create a brand comparison table.

    Args:
        dfs: Dict of {brand_name: store_dataframe}
        group_level: Geography level for comparison

    Returns:
        Wide-format comparison table with brands as columns.
    """
    summaries = []

    for brand, df in dfs.items():
        agg = aggregate_stores(df, group_level=group_level)
        if not agg.empty:
            agg["brand"] = brand
            summaries.append(agg)

    if not summaries:
        return pd.DataFrame()

    combined = pd.concat(summaries, ignore_index=True)
    return combined


def generate_executive_summary(df: pd.DataFrame, brand: str) -> str:
    """Generate a text summary for a brand's location footprint."""
    if df.empty:
        return f"No data available for {brand}."

    total_stores = len(df)
    cities = df["city"].nunique() if "city" in df.columns else 0
    avg_rating = df["rating"].mean() if "rating" in df.columns else 0
    total_reviews = df["review_count"].sum() if "review_count" in df.columns else 0

    top_city = ""
    if "city" in df.columns:
        city_counts = df["city"].value_counts()
        if len(city_counts) > 0:
            top_city = city_counts.index[0]

    summary = (
        f"{brand}: {total_stores} stores across {cities} cities. "
        f"Average rating: {avg_rating:.1f}/5 ({int(total_reviews):,} total reviews). "
    )

    if top_city:
        summary += f"Highest concentration in {top_city} ({city_counts.iloc[0]} stores)."

    if avg_rating >= 4.0 and total_stores < 50:
        summary += " High ratings with limited footprint suggest strong expansion potential."
    elif avg_rating < 3.5:
        summary += " Below-average ratings may indicate operational challenges to address before expansion."

    return summary
