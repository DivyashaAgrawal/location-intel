from __future__ import annotations

import json
import logging

import requests as http_requests

from src.core.config import OLLAMA_BASE_URL, OLLAMA_MODEL

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """You parse retail location queries for India into JSON. Output ONLY valid JSON, nothing else.

Schema:
{"query_type":"brand"|"category","brands":["..."],"category":"..."|null,"search_query":"...","geography":{"level":"pincode"|"city"|"state","filter":["city names"]},"metrics":["store_count","avg_rating","review_count","sentiment_summary"],"comparison":false}

Rules:
- brand query: user names a specific brand. Set query_type="brand", brands=["Brand Name"], search_query=brand name.
- category query: user asks about a type of store (pizza, coffee, jewelry). Set query_type="category", category="pizza", search_query="pizza restaurants", brands=[].
- Default geography filter: ["Delhi","Mumbai","Bangalore","Chennai","Hyderabad"]
- Default metrics: ["store_count","avg_rating"]
- If "summary" in query, include all 4 metrics.
- If "vs" or "compare" in query, set comparison=true.
- Normalize brands: dominos->Dominos Pizza, mcd/mcdonalds->McDonald's, kfc->KFC

Examples:
Input: "pizza stores in Delhi"
Output: {"query_type":"category","brands":[],"category":"pizza","search_query":"pizza restaurants","geography":{"level":"city","filter":["Delhi"]},"metrics":["store_count","avg_rating"],"comparison":false}

Input: "pincode wise Dominos in Mumbai with ratings"
Output: {"query_type":"brand","brands":["Dominos Pizza"],"category":null,"search_query":"Dominos Pizza","geography":{"level":"pincode","filter":["Mumbai"]},"metrics":["store_count","avg_rating"],"comparison":false}

Input: "compare KFC vs McDonald's in Delhi and Bangalore"
Output: {"query_type":"brand","brands":["KFC","McDonald's"],"category":null,"search_query":"KFC","geography":{"level":"city","filter":["Delhi","Bangalore"]},"metrics":["store_count","avg_rating"],"comparison":true}

Input: "dominos vs pizzahut in delhi"
Output: {"query_type":"brand","brands":["Dominos Pizza","Pizza Hut"],"category":null,"search_query":"Dominos Pizza","geography":{"level":"city","filter":["Delhi"]},"metrics":["store_count","avg_rating"],"comparison":true}

Input: "compare haldirams vs bikanervala in mumbai"
Output: {"query_type":"brand","brands":["Haldiram's","Bikanervala"],"category":null,"search_query":"Haldiram's","geography":{"level":"city","filter":["Mumbai"]},"metrics":["store_count","avg_rating"],"comparison":true}

Input: "summary of dominos vs pizzahut in delhi"
Output: {"query_type":"brand","brands":["Dominos Pizza","Pizza Hut"],"category":null,"search_query":"Dominos Pizza","geography":{"level":"city","filter":["Delhi"]},"metrics":["store_count","avg_rating","review_count","sentiment_summary"],"comparison":true}
"""

KNOWN_BRANDS = {
    "dominos": "Dominos Pizza",
    "domino's": "Dominos Pizza",
    "da milano": "Da Milano",
    "haldirams": "Haldiram's",
    "haldiram": "Haldiram's",
    "bikanervala": "Bikanervala",
    "mcdonald": "McDonald's",
    "mcdonalds": "McDonald's",
    "mcd": "McDonald's",
    "subway": "Subway",
    "starbucks": "Starbucks",
    "kfc": "KFC",
    "burger king": "Burger King",
    "pizza hut": "Pizza Hut",
    "pizzahut": "Pizza Hut",
    "zara": "Zara",
    "h&m": "H&M",
    "tanishq": "Tanishq",
    "titan": "Titan Eye Plus",
    "lenskart": "Lenskart",
    "firstcry": "FirstCry",
    "nykaa": "Nykaa",
    "fabindia": "FabIndia",
    "chaayos": "Chaayos",
    "chai point": "Chai Point",
    "wow momo": "Wow! Momo",
    "bata": "Bata",
    "raymond": "Raymond",
    "peter england": "Peter England",
}

GEOGRAPHY_KEYWORDS = {
    "pincode": ["pincode", "pin code", "zip", "postal"],
    "city": ["city", "cities", "city wise", "citywise"],
    "state": ["state", "states", "state wise", "statewise"],
    "district": ["district", "districts"],
    "national": ["national", "all india", "pan india", "india wide", "country"],
}

CATEGORY_KEYWORDS = {
    "pizza": {"triggers": ["pizza", "pizzeria"], "search_query": "pizza restaurants"},
    "coffee": {
        "triggers": ["coffee", "cafe", "cafes", "coffee shop"],
        "search_query": "coffee shops",
    },
    "burger": {"triggers": ["burger", "burgers"], "search_query": "burger restaurants"},
    "biryani": {"triggers": ["biryani"], "search_query": "biryani restaurants"},
    "chinese": {
        "triggers": ["chinese food", "chinese restaurant"],
        "search_query": "chinese restaurants",
    },
    "bakery": {
        "triggers": ["bakery", "bakeries", "cake shop"],
        "search_query": "bakeries",
    },
    "ice cream": {
        "triggers": ["ice cream", "gelato"],
        "search_query": "ice cream shops",
    },
    "sweet shop": {
        "triggers": ["sweet shop", "mithai", "sweets"],
        "search_query": "sweet shops",
    },
    "fast food": {
        "triggers": ["fast food", "qsr"],
        "search_query": "fast food restaurants",
    },
    "fine dining": {
        "triggers": ["fine dining", "premium restaurant"],
        "search_query": "fine dining restaurants",
    },
    "jewelry": {
        "triggers": ["jewelry", "jewellery", "jewellers", "jewelers"],
        "search_query": "jewelry stores",
    },
    "clothing": {
        "triggers": ["clothing", "apparel", "fashion", "garment"],
        "search_query": "clothing stores",
    },
    "footwear": {
        "triggers": ["footwear", "shoe", "shoes"],
        "search_query": "shoe stores",
    },
    "electronics": {
        "triggers": ["electronics", "mobile", "phone store"],
        "search_query": "electronics stores",
    },
    "eyewear": {
        "triggers": ["eyewear", "optical", "optician"],
        "search_query": "optical stores",
    },
    "pharmacy": {
        "triggers": ["pharmacy", "chemist", "medical store", "drug store"],
        "search_query": "pharmacies",
    },
    "grocery": {
        "triggers": ["grocery", "supermarket", "kirana"],
        "search_query": "grocery stores",
    },
    "gym": {"triggers": ["gym", "fitness", "fitness center"], "search_query": "gyms"},
    "salon": {
        "triggers": ["salon", "beauty parlour", "beauty parlor", "spa"],
        "search_query": "beauty salons",
    },
    "pet store": {"triggers": ["pet store", "pet shop"], "search_query": "pet stores"},
    "bookstore": {
        "triggers": ["bookstore", "book shop", "books"],
        "search_query": "bookstores",
    },
    "restaurant": {
        "triggers": ["restaurant", "restaurants", "eateries", "food"],
        "search_query": "restaurants",
    },
}

METRIC_KEYWORDS = {
    "store_count": [
        "stores",
        "store count",
        "outlets",
        "branches",
        "locations",
        "count",
    ],
    "avg_rating": ["rating", "ratings", "avg rating", "average rating", "stars"],
    "review_count": ["reviews", "review count", "# of reviews", "number of reviews"],
    "sentiment_summary": [
        "sentiment",
        "feedback",
        "good",
        "bad",
        "neutral",
        "positive",
        "negative",
    ],
    "address_list": ["address", "addresses", "location details", "full address"],
}


def parse_query_with_ollama(query: str) -> dict:
    """Use local Ollama (llama3.2 3B) to parse the query."""
    raw = ""
    try:
        response = http_requests.post(
            f"{OLLAMA_BASE_URL}/api/generate",
            json={
                "model": OLLAMA_MODEL,
                "prompt": f"{SYSTEM_PROMPT}\n\nInput: {query}\nOutput:",
                "stream": False,
                "options": {
                    "temperature": 0.1,  # low temp for deterministic parsing
                    "num_predict": 300,
                },
            },
            timeout=30,
        )
        response.raise_for_status()

        raw = response.json().get("response", "").strip()

        start = raw.find("{")
        end = raw.rfind("}") + 1
        if start >= 0 and end > start:
            raw = raw[start:end]

        parsed = json.loads(raw)

        required = ["query_type", "geography"]
        if not all(k in parsed for k in required):
            raise ValueError(
                f"Missing required fields: {[k for k in required if k not in parsed]}"
            )

        parsed.setdefault("brands", [])
        parsed.setdefault("category", None)
        parsed.setdefault("search_query", None)
        parsed.setdefault("metrics", ["store_count", "avg_rating"])
        parsed.setdefault("comparison", False)

        q_lower = query.lower()
        looks_like_compare = any(kw in q_lower for kw in ["vs", "versus", "compare"])
        if looks_like_compare and len(parsed.get("brands", [])) < 2:
            raise ValueError("compare query with <2 brands; deferring to fallback")

        return parsed

    except http_requests.ConnectionError:
        raise RuntimeError("Ollama not running. Start with: ollama serve")
    except json.JSONDecodeError as e:
        raise ValueError(f"LLM returned invalid JSON: {e}\nRaw: {raw[:200]}")


def parse_query_fallback(query: str) -> dict:
    """Rule-based fallback parser. No API needed."""
    import re

    query_lower = query.lower()

    detected_category = None
    search_query = None

    for cat_name, cat_info in CATEGORY_KEYWORDS.items():
        if any(trigger in query_lower for trigger in cat_info["triggers"]):
            detected_category = cat_name
            search_query = cat_info["search_query"]
            break

    brands = []
    for key, canonical in KNOWN_BRANDS.items():
        if key in query_lower and canonical not in brands:
            brands.append(canonical)

    # Explicit brand wins over category keywords.
    if brands:
        query_type = "brand"
        search_query = None
        detected_category = None
    elif detected_category:
        query_type = "category"
    else:
        quoted = re.findall(r'"([^"]+)"', query)
        if quoted:
            brands = quoted
            query_type = "brand"
        else:
            query_type = "category"
            search_query = (
                query_lower.replace("summary", "")
                .replace("give me", "")
                .replace("get me", "")
                .strip()
            )
            detected_category = "general"

    geo_level = "city"
    for level, keywords in GEOGRAPHY_KEYWORDS.items():
        if any(kw in query_lower for kw in keywords):
            geo_level = level
            break

    from src.core.config import INDIA_MAJOR_CITIES

    geo_filter = []
    for city in INDIA_MAJOR_CITIES:
        if city.lower() in query_lower:
            geo_filter.append(city)

    if not geo_filter:
        if any(
            phrase in query_lower for phrase in ["the city", "this city", "my city"]
        ):
            geo_filter = ["Delhi"]
        else:
            geo_filter = ["Delhi", "Mumbai", "Bangalore", "Chennai", "Hyderabad"]

    metrics = ["store_count", "avg_rating"]
    for metric, keywords in METRIC_KEYWORDS.items():
        if metric not in metrics and any(kw in query_lower for kw in keywords):
            metrics.append(metric)

    if "summary" in query_lower:
        metrics = list(METRIC_KEYWORDS.keys())

    comparison = any(
        kw in query_lower for kw in ["vs", "versus", "compare", "comparison", "against"]
    )

    return {
        "query_type": query_type,
        "brands": brands,
        "category": detected_category,
        "search_query": search_query,
        "geography": {"level": geo_level, "filter": geo_filter},
        "metrics": metrics,
        "comparison": comparison,
    }


def parse_query(query: str) -> dict:
    """
    Parse a natural language query.
    Priority: Ollama (local, free) -> rule-based fallback.
    """
    try:
        result = parse_query_with_ollama(query)
        return result
    except RuntimeError as e:
        logger.warning(f"  Ollama unavailable ({e}), using rule-based parser")
    except Exception as e:
        logger.warning(f"  Ollama parse failed ({e}), using rule-based parser")

    logger.info("[NLU: rule-based]")
    return parse_query_fallback(query)
