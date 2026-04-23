from __future__ import annotations

import pandas as pd

# Census 2011 + estimates. Used for per-capita store density calculations.
CITY_POPULATION = {
    "Delhi": 16_787_941,
    "Mumbai": 12_442_373,
    "Bangalore": 8_443_675,
    "Chennai": 7_088_000,
    "Hyderabad": 6_809_970,
    "Kolkata": 4_496_694,
    "Pune": 3_124_458,
    "Ahmedabad": 5_570_585,
    "Jaipur": 3_046_163,
    "Lucknow": 2_815_601,
    "Chandigarh": 1_055_450,
    "Indore": 1_994_397,
    "Bhopal": 1_798_218,
    "Patna": 1_684_222,
    "Nagpur": 2_405_421,
    "Coimbatore": 1_601_438,
    "Kochi": 677_381,
    "Gurgaon": 876_969,
    "Noida": 642_381,
    "Surat": 4_467_797,
}

CITY_TIERS = {
    "Tier 1": ["Delhi", "Mumbai", "Bangalore", "Chennai", "Hyderabad", "Kolkata"],
    "Tier 2": ["Pune", "Ahmedabad", "Jaipur", "Lucknow", "Surat", "Nagpur", "Indore", "Bhopal", "Patna", "Coimbatore"],
    "Tier 3": ["Chandigarh", "Kochi", "Gurgaon", "Noida"],
}


def get_city_tier(city: str) -> str:
    for tier, cities in CITY_TIERS.items():
        if city in cities:
            return tier
    return "Other"


def compute_store_density(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate stores per 100K population for each city.
    Key metric for PE: tells you market penetration at a glance.
    """
    if df.empty:
        return pd.DataFrame()

    city_stores = df.groupby(["brand", "city"]).agg(
        store_count=("title", "count"),
        avg_rating=("rating", "mean"),
        total_reviews=("review_count", "sum"),
    ).reset_index()

    city_stores["population"] = city_stores["city"].map(CITY_POPULATION)
    city_stores["stores_per_100k"] = (
        city_stores["store_count"] / city_stores["population"] * 100_000
    ).round(2)
    city_stores["city_tier"] = city_stores["city"].apply(get_city_tier)
    city_stores["avg_rating"] = city_stores["avg_rating"].round(1)

    return city_stores.sort_values("stores_per_100k", ascending=False)


def whitespace_analysis(
    brand_df: pd.DataFrame,
    all_target_cities: list[str] | None = None,
) -> pd.DataFrame:
    """
    Identify cities where the brand has NO stores or is under-penetrated.

    This is the money slide for PE: "Brand X has 0 stores in Surat (pop 4.5M)
    but 15 stores in Pune (pop 3.1M). Clear expansion opportunity."
    """
    if all_target_cities is None:
        all_target_cities = list(CITY_POPULATION.keys())

    brand = brand_df["brand"].iloc[0] if not brand_df.empty else "Unknown"

    present_cities = set(brand_df["city"].unique()) if not brand_df.empty else set()

    results = []
    for city in all_target_cities:
        population = CITY_POPULATION.get(city, 0)
        city_data = brand_df[brand_df["city"] == city] if not brand_df.empty else pd.DataFrame()
        store_count = len(city_data)

        stores_per_100k = (store_count / population * 100_000) if population > 0 else 0

        if not brand_df.empty and len(present_cities) > 0:
            total_stores = len(brand_df)
            total_pop = sum(CITY_POPULATION.get(c, 0) for c in present_cities)
            avg_density = (total_stores / total_pop * 100_000) if total_pop > 0 else 0
        else:
            avg_density = 0

        if store_count == 0 and population > 1_000_000:
            opportunity = "High - No presence in large market"
            score = 90 + min(10, population / 1_000_000)
        elif store_count == 0:
            opportunity = "Medium - No presence"
            score = 60
        elif stores_per_100k < avg_density * 0.5:
            opportunity = "Medium - Under-penetrated vs average"
            score = 50 + (1 - stores_per_100k / max(avg_density, 0.01)) * 30
        else:
            opportunity = "Low - Adequate coverage"
            score = 20

        results.append({
            "brand": brand,
            "city": city,
            "city_tier": get_city_tier(city),
            "population": population,
            "current_stores": store_count,
            "stores_per_100k": round(stores_per_100k, 2),
            "avg_brand_density": round(avg_density, 2),
            "opportunity": opportunity,
            "expansion_score": round(min(100, score), 1),
        })

    result_df = pd.DataFrame(results)
    return result_df.sort_values("expansion_score", ascending=False)


def peer_benchmark(
    brand_dfs: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """
    Compare multiple brands side by side.

    PE firms love this: "Dominos has 3.2 stores per 100K in Delhi,
    McDonald's has 2.8. But McDonald's avg rating is 4.2 vs Dominos 3.9."
    """
    rows = []

    for brand, df in brand_dfs.items():
        if df.empty:
            continue

        total_stores = len(df)
        cities = df["city"].nunique()
        avg_rating = df["rating"].mean() if "rating" in df.columns else 0
        total_reviews = df["review_count"].sum() if "review_count" in df.columns else 0
        avg_reviews_per_store = total_reviews / max(total_stores, 1)

        tier1_cities = set(CITY_TIERS["Tier 1"])
        brand_cities = set(df["city"].unique())
        tier1_coverage = len(brand_cities & tier1_cities) / len(tier1_cities) * 100

        covered_pop = sum(CITY_POPULATION.get(c, 0) for c in brand_cities)
        total_pop = sum(CITY_POPULATION.values())
        pop_coverage = covered_pop / total_pop * 100

        rows.append({
            "brand": brand,
            "total_stores": total_stores,
            "cities_present": cities,
            "avg_rating": round(avg_rating, 1),
            "total_reviews": int(total_reviews),
            "avg_reviews_per_store": int(avg_reviews_per_store),
            "tier1_coverage_%": round(tier1_coverage, 0),
            "population_coverage_%": round(pop_coverage, 1),
        })

    return pd.DataFrame(rows).sort_values("total_stores", ascending=False)


def generate_ic_memo_points(
    brand: str,
    density_df: pd.DataFrame,
    whitespace_df: pd.DataFrame,
) -> list[str]:
    """
    Analytical observations for an IC memo.

    Each bullet must state an interpretation, risk, or implication -- not a
    number already in `density_df` / `whitespace_df`. If no analytical claim
    is warranted for a given dimension, that dimension is skipped.
    """
    points: list[str] = []

    if not density_df.empty:
        total_stores = float(density_df["store_count"].sum())
        if total_stores > 0:
            top_share = float(density_df.iloc[0]["store_count"]) / total_stores
            if top_share >= 0.4:
                points.append(
                    f"Footprint concentration risk: {top_share * 100:.0f}% of "
                    f"stores sit in {density_df.iloc[0]['city']}. Single-market "
                    "shocks (rent, hiring, regulation) hit P&L disproportionately."
                )

        if "stores_per_100k" in density_df.columns and len(density_df) >= 3:
            median_density = float(density_df["stores_per_100k"].median())
            top_density = float(density_df.iloc[0]["stores_per_100k"])
            if median_density > 0 and top_density / median_density >= 2.5:
                points.append(
                    f"{density_df.iloc[0]['city']} is saturated relative to the "
                    f"rest of the network ({top_density:.1f} vs {median_density:.1f} "
                    "median per 100K); incremental capex is better deployed in "
                    "lower-density markets."
                )

        if "avg_rating" in density_df.columns:
            ratings = density_df["avg_rating"].dropna()
            if not ratings.empty:
                avg_rating = float(ratings.mean())
                if avg_rating >= 4.2:
                    points.append(
                        f"Brand equity supports premium pricing and new-market entry "
                        f"(avg {avg_rating:.1f}/5); expansion thesis carries low "
                        "demand-side risk."
                    )
                elif avg_rating < 3.5:
                    points.append(
                        f"Customer experience is a blocker before expansion "
                        f"(avg {avg_rating:.1f}/5). Fix operations in the existing "
                        "base before underwriting new stores."
                    )
                if len(ratings) >= 3:
                    rating_spread = float(ratings.max() - ratings.min())
                    if rating_spread >= 0.8:
                        points.append(
                            f"Execution quality varies by market (rating spread "
                            f"{rating_spread:.1f}). Diligence should isolate whether "
                            "weak markets are franchise/ops issues or trade-area mismatch."
                        )

    if not whitespace_df.empty and "current_stores" in whitespace_df.columns:
        zero_presence = whitespace_df[whitespace_df["current_stores"] == 0]
        if "population" in zero_presence.columns:
            large_zero = zero_presence[zero_presence["population"] > 2_000_000]
            if len(large_zero) >= 2:
                points.append(
                    "Structural whitespace in multiple 2M+ population cities "
                    f"({', '.join(large_zero['city'].head(5).tolist())}) suggests "
                    "the brand is under-earning its TAM; absence is likely a "
                    "distribution problem, not a demand problem."
                )

    return points
