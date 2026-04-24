"""
Competitor analysis.

For a focal brand, auto-identify direct competitors, fetch their store
footprints, and classify each pincode into one of four territories:

- Defensible territory    -- focal brand is present, competitor is not
- Contested               -- both brands present
- Competitor whitespace   -- competitor present, focal brand is not
- Open market             -- neither present (currently not surfaced; kept for symmetry)

Outputs are three DataFrames + a list of memo bullets suitable for an IC deck.
"""
from __future__ import annotations

import logging
from collections.abc import Callable

import pandas as pd

logger = logging.getLogger(__name__)



# Minimal hand-curated competitor map. Keep tight -- PE IC work cares about
# direct substitutes, not loose category overlap.
COMPETITOR_MAP: dict[str, list[str]] = {
    "Dominos Pizza":  ["Pizza Hut", "La Pino'z", "Mojo Pizza", "Papa John's", "Oven Story"],
    "Pizza Hut":      ["Dominos Pizza", "La Pino'z", "Papa John's"],
    "McDonald's":     ["Burger King", "KFC", "Subway", "Wendy's"],
    "Burger King":    ["McDonald's", "KFC", "Wendy's"],
    "KFC":            ["McDonald's", "Burger King", "Popeyes"],
    "Subway":         ["McDonald's", "Burger King"],
    "Starbucks":      ["Blue Tokai", "Third Wave Coffee", "Chaayos", "Cafe Coffee Day", "Barista"],
    "Chaayos":        ["Chai Point", "Cafe Coffee Day", "Starbucks"],
    "Chai Point":     ["Chaayos", "Cafe Coffee Day"],
    "Cafe Coffee Day":["Starbucks", "Chaayos", "Blue Tokai", "Barista"],
    "Haldiram's":     ["Bikanervala", "Chaayos", "Wow! Momo"],
    "Bikanervala":    ["Haldiram's"],
    "Da Milano":      ["Hidesign", "Louis Philippe"],
    "Tanishq":        ["Malabar Gold", "Kalyan Jewellers", "PC Jeweller", "CaratLane"],
    "Malabar Gold":   ["Tanishq", "Kalyan Jewellers", "CaratLane"],
    "Lenskart":       ["Titan Eye Plus", "Specsmakers"],
    "Titan Eye Plus": ["Lenskart"],
    "FabIndia":       ["Westside", "Pantaloons", "Max Fashion"],
    "Zara":           ["H&M", "Westside", "Max Fashion"],
    "H&M":            ["Zara", "Pantaloons", "Westside"],
    "Bata":           ["Metro Shoes", "Liberty", "Relaxo", "Woodland"],
    "Nykaa":          ["Sephora", "MyGlamm"],
    "Wow! Momo":      ["Haldiram's", "Chaayos"],
}


def _norm_brand(s: str) -> str:
    """Case + punctuation insensitive key for fuzzy brand matching."""
    return (s or "").strip().lower().replace("'", "").replace("'", "")


# Reverse lookup: brand -> category, for merging with discovered_competitors.
# Conservative mapping; only entries where the category is unambiguous.
BRAND_CATEGORY: dict[str, str] = {
    "Dominos Pizza":   "pizza",
    "Pizza Hut":       "pizza",
    "La Pino'z":       "pizza",
    "Mojo Pizza":      "pizza",
    "Papa John's":     "pizza",
    "Oven Story":      "pizza",
    "McDonald's":      "burger",
    "Burger King":     "burger",
    "KFC":             "fried_chicken",
    "Subway":          "sandwich",
    "Starbucks":       "coffee",
    "Cafe Coffee Day": "coffee",
    "Barista":         "coffee",
    "Blue Tokai":      "coffee",
    "Third Wave Coffee": "coffee",
    "Chaayos":         "tea",
    "Chai Point":      "tea",
    "Haldiram's":      "indian_qsr",
    "Bikanervala":     "indian_qsr",
    "Wow! Momo":       "indian_qsr",
    "Da Milano":       "leather",
    "Tanishq":         "jewellery",
    "Malabar Gold":    "jewellery",
    "Kalyan Jewellers": "jewellery",
    "CaratLane":       "jewellery",
    "Lenskart":        "eyewear",
    "Titan Eye Plus":  "eyewear",
    "FabIndia":        "apparel",
    "Zara":            "apparel",
    "H&M":             "apparel",
    "Westside":        "apparel",
    "Pantaloons":      "apparel",
    "Max Fashion":     "apparel",
    "Bata":            "footwear",
    "Metro Shoes":     "footwear",
    "Liberty":         "footwear",
    "Relaxo":          "footwear",
    "Woodland":        "footwear",
    "Nykaa":           "beauty",
}


def get_competitors(brand: str, max_n: int = 3) -> list[str]:
    """
    Direct competitors for `brand`. Returns the hand-curated list merged
    with anything in `discovered_competitors` for the same category.
    Matching is case-insensitive and tolerant of minor variations.
    """
    if not brand:
        return []

    key = _norm_brand(brand)
    static: list[str] = []
    for b, comps in COMPETITOR_MAP.items():
        if _norm_brand(b) == key:
            static = list(comps)
            break
    if not static:
        for b, comps in COMPETITOR_MAP.items():
            lk = _norm_brand(b)
            if key in lk or lk in key:
                static = list(comps)
                break

    category = _lookup_category(brand)
    discovered: list[str] = []
    if category:
        try:
            from src.cache import db as _db
            rows = _db.get_discovered_competitors(category)
        except Exception:
            rows = []
        for row in rows:
            name = row.get("brand")
            if name and _norm_brand(name) != key:
                discovered.append(name)

    seen: set[str] = set()
    merged: list[str] = []
    for name in static + discovered:
        nk = _norm_brand(name)
        if nk and nk not in seen:
            seen.add(nk)
            merged.append(name)

    return merged[:max_n]


def _lookup_category(brand: str) -> str | None:
    key = _norm_brand(brand)
    for b, cat in BRAND_CATEGORY.items():
        if _norm_brand(b) == key:
            return cat
    for b, cat in BRAND_CATEGORY.items():
        lk = _norm_brand(b)
        if key in lk or lk in key:
            return cat
    return None


# ---------------------------------------------------------------------------
# Territory classification
# ---------------------------------------------------------------------------

def classify_territory(
    focal_stores: pd.DataFrame,
    competitor_stores: pd.DataFrame,
    group_by: str = "pincode",
) -> pd.DataFrame:
    """
    Given focal and competitor store DataFrames, return a per-`group_by`
    classification: Defensible / Contested / Competitor whitespace / Open.

    Both inputs should have the grouping column (default `pincode`).
    """
    focal_pincodes = (
        set(focal_stores[group_by].dropna().astype(str))
        if not focal_stores.empty and group_by in focal_stores.columns
        else set()
    )
    comp_pincodes = (
        set(competitor_stores[group_by].dropna().astype(str))
        if not competitor_stores.empty and group_by in competitor_stores.columns
        else set()
    )

    rows = []
    for pc in sorted(focal_pincodes | comp_pincodes):
        in_focal = pc in focal_pincodes
        in_comp = pc in comp_pincodes

        focal_count = int(
            (focal_stores[group_by].astype(str) == pc).sum()
            if not focal_stores.empty and group_by in focal_stores.columns
            else 0
        )
        comp_count = int(
            (competitor_stores[group_by].astype(str) == pc).sum()
            if not competitor_stores.empty and group_by in competitor_stores.columns
            else 0
        )

        if in_focal and not in_comp:
            territory = "Defensible territory"
        elif in_focal and in_comp:
            territory = "Contested"
        elif in_comp and not in_focal:
            territory = "Competitor whitespace"
        else:
            territory = "Open market"

        rows.append({
            group_by: pc,
            "territory": territory,
            "focal_stores": focal_count,
            "competitor_stores": comp_count,
        })

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Share of voice
# ---------------------------------------------------------------------------

def competitor_share_of_voice(
    focal_brand: str,
    brand_frames: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """
    Given {brand_name: stores_df}, return per-brand store counts and share
    of voice as a percentage. The focal brand is flagged so the UI can
    render it distinctly.
    """
    rows = []
    total = sum(len(df) for df in brand_frames.values() if df is not None and not df.empty)
    for brand, df in brand_frames.items():
        n = 0 if df is None or df.empty else len(df)
        share = (n / total * 100) if total else 0.0
        rows.append({
            "brand": brand,
            "store_count": n,
            "share_of_voice_%": round(share, 1),
            "is_focal_brand": brand == focal_brand,
        })
    return pd.DataFrame(rows).sort_values(
        "store_count", ascending=False
    ).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Memo generation
# ---------------------------------------------------------------------------

def generate_competitor_memo_points(
    focal_brand: str,
    territory_df: pd.DataFrame,
    sov_df: pd.DataFrame,
) -> list[str]:
    """
    Analytical takeaways on the competitive picture. Each bullet states an
    interpretation or implication, not a statistic already in the tables.
    """
    points: list[str] = []

    if sov_df is not None and not sov_df.empty:
        focal_row = sov_df[sov_df["is_focal_brand"]]
        non_focal = sov_df[~sov_df["is_focal_brand"]]
        if not focal_row.empty and not non_focal.empty:
            focal_sov = float(focal_row.iloc[0]["share_of_voice_%"])
            top_comp_sov = float(non_focal.iloc[0]["share_of_voice_%"])
            gap = focal_sov - top_comp_sov

            if focal_sov >= 45:
                points.append(
                    f"{focal_brand} is the category anchor, not a challenger; "
                    "pricing power and supplier terms should reflect that position."
                )
            elif gap >= 10:
                points.append(
                    f"{focal_brand} leads the mapped set, but the next competitor "
                    f"({non_focal.iloc[0]['brand']}) is within striking distance "
                    f"({gap:.0f}pp gap); defensive investment matters more than expansion."
                )
            elif gap <= -10:
                points.append(
                    f"{focal_brand} is the challenger to {non_focal.iloc[0]['brand']} "
                    f"({-gap:.0f}pp behind on share of voice); thesis requires a "
                    "credible route to catch up, not just organic growth."
                )
            else:
                points.append(
                    "Market is fragmented at the top (no brand holds a decisive "
                    "share lead); share gains come from execution, not positioning."
                )

    if territory_df is not None and not territory_df.empty:
        defensible = int((territory_df["territory"] == "Defensible territory").sum())
        contested = int((territory_df["territory"] == "Contested").sum())
        whitespace = int((territory_df["territory"] == "Competitor whitespace").sum())
        total = defensible + contested + whitespace

        if total > 0:
            if contested / total >= 0.5:
                points.append(
                    "Majority of pincodes are contested; margin compression from "
                    "discounting and co-location is the default state, not an exception."
                )
            if defensible >= 3 * max(whitespace, 1) and defensible >= 3:
                points.append(
                    f"{focal_brand} has a real geographic moat -- defensible "
                    "pincodes far outnumber competitor whitespace. Protect these "
                    "before chasing new markets."
                )
            if whitespace >= 2 * max(defensible, 1) and whitespace >= 3:
                points.append(
                    f"Competitors have built ahead of {focal_brand} in more "
                    "pincodes than the reverse; any expansion thesis needs to "
                    "acknowledge this is a catch-up investment, not greenfield."
                )

    return points


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def run_competitor_analysis(
    focal_brand: str,
    focal_stores: pd.DataFrame,
    cities: list[str],
    fetch_fn: Callable[[str, str], tuple[pd.DataFrame, str]],
    max_competitors: int = 3,
) -> dict:
    """
    Run the full competitor flow for a focal brand.

    `fetch_fn` is typically `cache_manager.smart_fetch`. It must take
    (brand, city) and return (DataFrame, source_label). This keeps the
    competitor module decoupled from the cache layer -- easy to test.

    Returns a dict:
        {
            "competitors": [...],
            "competitor_stores": {competitor_brand: df},
            "share_of_voice": DataFrame,
            "territory_by_pincode": DataFrame,
            "memo_points": [str, ...],
        }
    """
    competitors = get_competitors(focal_brand, max_n=max_competitors)

    category = _lookup_category(focal_brand)
    tentative_flags: dict[str, bool] = {}
    if category:
        try:
            from src.cache import db as _db
            rows = _db.get_discovered_competitors(category)
        except Exception:
            rows = []
        for row in rows:
            times_seen = int(row.get("times_seen") or 0)
            verified = int(row.get("manually_verified") or 0)
            if times_seen < 3 and not verified:
                tentative_flags[row["brand"]] = True

    competitor_frames: dict[str, pd.DataFrame] = {}
    for comp in competitors:
        frames = []
        for city in cities:
            try:
                df, _src = fetch_fn(comp, city)
            except Exception as e:
                logger.warning(f"    [competitor] fetch error for {comp} in {city}: {e}")
                df = pd.DataFrame()
            if df is not None and not df.empty:
                frames.append(df)
        if frames:
            competitor_frames[comp] = pd.concat(frames, ignore_index=True)
        else:
            competitor_frames[comp] = pd.DataFrame()

    combined_competitors = (
        pd.concat(competitor_frames.values(), ignore_index=True)
        if competitor_frames
        else pd.DataFrame()
    )

    brand_frames = {focal_brand: focal_stores, **competitor_frames}
    sov_df = competitor_share_of_voice(focal_brand, brand_frames)

    territory_df = classify_territory(
        focal_stores, combined_competitors, group_by="pincode"
    )

    memo = generate_competitor_memo_points(focal_brand, territory_df, sov_df)

    return {
        "competitors": competitors,
        "competitor_stores": competitor_frames,
        "share_of_voice": sov_df,
        "territory_by_pincode": territory_df,
        "memo_points": memo,
        "tentative_competitors": [c for c in competitors if tentative_flags.get(c)],
    }
