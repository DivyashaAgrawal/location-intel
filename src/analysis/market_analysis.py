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
    Generate bullet points for an IC memo.
    These are the insights a PE analyst would write up.
    """
    points = []

    if not density_df.empty:
        top_city = density_df.iloc[0]
        points.append(
            f"Highest penetration in {top_city['city']} "
            f"({top_city['stores_per_100k']:.1f} stores per 100K population, "
            f"{int(top_city['store_count'])} stores)"
        )

        avg_rating = density_df["avg_rating"].mean()
        if avg_rating >= 4.0:
            points.append(
                f"Strong customer satisfaction (avg {avg_rating:.1f}/5 across all markets), "
                f"indicating healthy unit economics and brand loyalty"
            )
        elif avg_rating < 3.5:
            points.append(
                f"Customer satisfaction concern (avg {avg_rating:.1f}/5), "
                f"suggesting operational issues that need resolution before expansion"
            )

    if not whitespace_df.empty:
        high_opp = whitespace_df[whitespace_df["expansion_score"] >= 80]
        if len(high_opp) > 0:
            cities_list = ", ".join(high_opp["city"].head(5).tolist())
            points.append(
                f"{len(high_opp)} high-potential expansion markets identified: {cities_list}"
            )

        zero_presence = whitespace_df[whitespace_df["current_stores"] == 0]
        large_zero = zero_presence[zero_presence["population"] > 2_000_000]
        if len(large_zero) > 0:
            points.append(
                f"Zero presence in {len(large_zero)} cities with 2M+ population, "
                f"representing significant untapped market: "
                f"{', '.join(large_zero['city'].tolist())}"
            )

    return points
