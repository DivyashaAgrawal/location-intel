"""
Serper.dev Maps adapter.

Fallback to Google Places v1 (not used when Places returns results for the
same city in the same run -- enforced at the orchestrator). Every call logs
to `api_call_log`.
"""
from __future__ import annotations

import logging

import requests

from src.core import db
from src.core.config import INDIA_MAJOR_CITIES, SERPER_API_KEY
from src.fetchers._common import extract_brand_from_title, extract_pincode

logger = logging.getLogger(__name__)

SERPER_MAPS_URL = "https://google.serper.dev/maps"


def fetch(query: str, city: str, num_results: int = 40) -> list[dict]:
    """Serper Maps text search. Returns [] when no API key is set."""
    if not SERPER_API_KEY:
        return []

    lat, lng = None, None
    if city in INDIA_MAJOR_CITIES:
        lat = INDIA_MAJOR_CITIES[city]["lat"]
        lng = INDIA_MAJOR_CITIES[city]["lng"]

    payload: dict = {"q": f"{query} in {city}, India", "num": num_results}
    if lat and lng:
        payload["ll"] = f"@{lat},{lng},12z"

    headers = {"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"}

    success = False
    try:
        resp = requests.post(SERPER_MAPS_URL, json=payload, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        success = True
    except Exception:
        db.log_api_call("serper", brand=query, city=city, success=False, cost=0.0)
        raise
    finally:
        if success:
            db.log_api_call("serper", brand=query, city=city, success=True)

    results = []
    for place in data.get("places", []):
        title = place.get("title", "")
        brand = extract_brand_from_title(title)
        results.append({
            "source": "serper",
            "brand": brand,
            "title": title,
            "address": place.get("address", ""),
            "city": city,
            "state": None,
            "pincode": extract_pincode(place.get("address", "")),
            "latitude": place.get("latitude"),
            "longitude": place.get("longitude"),
            "rating": place.get("rating"),
            "review_count": place.get("ratingCount"),
            "phone": place.get("phoneNumber"),
            "website": place.get("website"),
            "category": place.get("category"),
            "reviews_text": None,
            "confidence": 0.9,
        })

    return results
