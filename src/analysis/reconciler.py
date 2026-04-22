"""
Reconciliation engine: The core intelligence layer.

Takes raw records from multiple sources (Serper, OSM, brand website) and:
1. Deduplicates stores across sources (same physical location)
2. Merges fields: picks the best value per field based on source priority
3. Fills gaps: if Serper has no pincode but the website does, use the website's
4. Scores data quality per store
5. Flags conflicts for review
"""
from __future__ import annotations

from math import asin, cos, radians, sin, sqrt

import pandas as pd

SOURCE_PRIORITY = {
    "google_places": {
        "rating": 1.0, "review_count": 1.0, "address": 0.9,
        "phone": 0.9, "website": 0.9, "pincode": 0.9,
        "brand": 0.95, "category": 0.9,
    },
    "serper": {
        "rating": 0.9, "review_count": 0.9, "address": 0.8,
        "phone": 0.8, "website": 0.85, "pincode": 0.7,
        "brand": 0.9, "category": 0.8,
    },
    "outscraper": {
        "rating": 0.95, "review_count": 0.95, "address": 0.9,
        "phone": 0.9, "website": 0.9, "pincode": 0.8,
        "brand": 0.9, "reviews_text": 1.0, "category": 0.9,
    },
    "osm": {
        "rating": 0.0, "review_count": 0.0, "address": 0.7,
        "phone": 0.5, "website": 0.6, "pincode": 0.8,
        "brand": 0.7, "category": 0.6,
    },
    "brand_website": {
        "rating": 0.3, "review_count": 0.0, "address": 1.0,
        "phone": 0.9, "website": 0.95, "pincode": 0.9,
        "brand": 1.0, "category": 0.7,
    },
}


def haversine_m(lat1, lon1, lat2, lon2) -> float:
    """Distance in meters between two lat/lng points."""
    if any(v is None or (isinstance(v, float) and pd.isna(v)) for v in [lat1, lon1, lat2, lon2]):
        return float("inf")
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    return 2 * 6371000 * asin(sqrt(a))


def deduplicate_cross_source(df, distance_threshold_m=100):
    """
    Cluster duplicate stores across sources by proximity + brand match.
    Returns DataFrame with 'cluster_id' column.
    """
    if df.empty:
        df["cluster_id"] = pd.Series(dtype=int)
        return df

    df = df.copy()
    df["cluster_id"] = -1
    cluster = 0

    df = df.sort_values("confidence", ascending=False).reset_index(drop=True)

    for i in range(len(df)):
        if df.at[i, "cluster_id"] >= 0:
            continue
        df.at[i, "cluster_id"] = cluster

        for j in range(i + 1, len(df)):
            if df.at[j, "cluster_id"] >= 0:
                continue

            bi = str(df.at[i, "brand"]).lower().strip()
            bj = str(df.at[j, "brand"]).lower().strip()
            brand_match = bi == bj or bi in bj or bj in bi

            if not brand_match:
                continue

            dist = haversine_m(
                df.at[i, "latitude"], df.at[i, "longitude"],
                df.at[j, "latitude"], df.at[j, "longitude"],
            )
            if dist <= distance_threshold_m:
                df.at[j, "cluster_id"] = cluster

        cluster += 1

    return df


def _best_value(group, field):
    """Pick the best non-null value for a field based on source priority."""
    candidates = []
    for _, row in group.iterrows():
        val = row.get(field)
        if val is None or (isinstance(val, float) and pd.isna(val)):
            continue
        if isinstance(val, str) and val.strip() == "":
            continue
        source = row.get("source", "unknown")
        priority = SOURCE_PRIORITY.get(source, {}).get(field, 0.5)
        candidates.append((val, priority))

    if not candidates:
        return None
    candidates.sort(key=lambda x: x[1], reverse=True)
    return candidates[0][0]


def reconcile(df):
    """
    Reconcile multi-source records into unified store records.
    One row per physical store, best value per field, with provenance tracking.

    Idempotent: a frame without a raw `source` column (i.e. already reconciled
    upstream, so only `sources` plural is present) is returned unchanged.
    """
    if df.empty:
        return df

    if "source" not in df.columns or not df["source"].notna().any():
        return df.reset_index(drop=True)

    df = deduplicate_cross_source(df)

    merge_fields = [
        "brand", "title", "address", "city", "state", "pincode",
        "latitude", "longitude", "rating", "review_count",
        "phone", "website", "category",
    ]

    merged = []
    for cid, group in df.groupby("cluster_id"):
        row = {f: _best_value(group, f) for f in merge_fields}
        row["sources"] = ",".join(sorted(group["source"].unique()))
        row["source_count"] = group["source"].nunique()

        key_fields = ["brand", "address", "pincode", "rating", "review_count", "phone"]
        filled = sum(1 for f in key_fields if row.get(f) is not None)
        row["data_quality"] = round(filled / len(key_fields) * 100, 0)

        reviews = []
        for _, r in group.iterrows():
            if r.get("reviews_text") and isinstance(r["reviews_text"], list):
                reviews.extend(r["reviews_text"])
        row["reviews_text"] = reviews if reviews else None
        row["confidence"] = round(group["confidence"].mean(), 2)

        merged.append(row)

    result = pd.DataFrame(merged)
    if "data_quality" in result.columns:
        result = result.sort_values("data_quality", ascending=False)
    return result.reset_index(drop=True)


def reconcile_sources(
    maps_df: pd.DataFrame,
    website_df: pd.DataFrame,
    brand: str,
) -> pd.DataFrame:
    """
    Reconcile store data from Google Maps and a brand's website.

    Adds the required 'source' and 'confidence' columns, combines both
    DataFrames, deduplicates by proximity + brand match, and merges fields
    using source priority.
    """
    maps = maps_df.copy() if not maps_df.empty else pd.DataFrame()
    web = website_df.copy() if not website_df.empty else pd.DataFrame()

    if not maps.empty:
        if "source" not in maps.columns:
            maps["source"] = "serper"
        if "confidence" not in maps.columns:
            maps["confidence"] = 0.9

    if not web.empty:
        if "source" not in web.columns:
            web["source"] = "brand_website"
        if "confidence" not in web.columns:
            web["confidence"] = 0.8

    if maps.empty and web.empty:
        return pd.DataFrame()

    combined = pd.concat([maps, web], ignore_index=True)
    combined["brand"] = combined["brand"].fillna(brand)

    return reconcile(combined)


def generate_reconciliation_summary(
    reconciled_df: pd.DataFrame,
    brand: str,
) -> dict:
    """
    Produce a reconciliation summary with counts of matched, maps-only,
    and website-only stores.
    """
    if reconciled_df.empty:
        return {
            "brand": brand,
            "total_unique": 0,
            "matched_both_sources": 0,
            "maps_only": 0,
            "website_only": 0,
        }

    sources_col = reconciled_df.get("sources")
    if sources_col is None:
        return {
            "brand": brand,
            "total_unique": len(reconciled_df),
            "matched_both_sources": 0,
            "maps_only": len(reconciled_df),
            "website_only": 0,
        }

    both = sources_col.str.contains("serper") & sources_col.str.contains("brand_website")
    maps_only = sources_col.str.contains("serper") & ~sources_col.str.contains("brand_website")
    website_only = sources_col.str.contains("brand_website") & ~sources_col.str.contains("serper")

    return {
        "brand": brand,
        "total_unique": len(reconciled_df),
        "matched_both_sources": int(both.sum()),
        "maps_only": int(maps_only.sum()),
        "website_only": int(website_only.sum()),
    }


def reconciliation_report(raw_df, merged_df):
    """Summary of the reconciliation for debugging and demo."""
    if raw_df.empty:
        return {"status": "no data"}

    # Already-reconciled input (no raw `source` column): nothing to summarise
    # against the raw side, so emit a compact "served from cache/db" shape.
    if "source" not in raw_df.columns or not raw_df["source"].notna().any():
        return {
            "status": "served from cache/db",
            "total_records": len(merged_df),
            "note": "already reconciled upstream",
        }

    report = {
        "total_raw_records": len(raw_df),
        "unique_stores_after_merge": len(merged_df),
        "dedup_ratio": f"{(1 - len(merged_df)/max(len(raw_df),1))*100:.1f}% duplicates removed",
        "sources_used": raw_df["source"].unique().tolist(),
        "records_per_source": raw_df["source"].value_counts().to_dict(),
        "avg_data_quality": f"{merged_df['data_quality'].mean():.0f}%" if "data_quality" in merged_df.columns else "N/A",
        "multi_source_stores": int((merged_df["source_count"] > 1).sum()) if "source_count" in merged_df.columns else 0,
        "fields_coverage": {},
    }

    for field in ["rating", "review_count", "pincode", "phone", "website"]:
        if field in merged_df.columns:
            filled = merged_df[field].notna().sum()
            report["fields_coverage"][field] = f"{filled}/{len(merged_df)} ({filled/max(len(merged_df),1)*100:.0f}%)"

    return report
