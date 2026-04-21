"""
OpenStreetMap Overpass adapter.

Free, no key. Used for gap-fill + cross-validation; returns [] for cities
outside `INDIA_MAJOR_CITIES` to avoid unbounded queries.
"""
from __future__ import annotations

import logging
import re

import requests

from src.core.config import INDIA_MAJOR_CITIES
from src.fetchers._common import extract_brand_from_title

logger = logging.getLogger(__name__)

OVERPASS_URL = "https://overpass-api.de/api/interpreter"


def fetch(query: str, city: str, radius_m: int = 15000) -> list[dict]:
    """Overpass POI query centred on the city. Returns [] for unknown cities."""
    if city not in INDIA_MAJOR_CITIES:
        return []
    lat = INDIA_MAJOR_CITIES[city]["lat"]
    lng = INDIA_MAJOR_CITIES[city]["lng"]

    # Prevent Overpass QL injection by aggressively sanitising the query.
    safe_query = re.sub(r"[^a-zA-Z0-9 &'\-]", "", query)

    overpass_query = f"""
    [out:json][timeout:25];
    (
      node["name"~"{safe_query}",i](around:{radius_m},{lat},{lng});
      way["name"~"{safe_query}",i](around:{radius_m},{lat},{lng});
    );
    out center body;
    """

    try:
        resp = requests.post(
            OVERPASS_URL,
            data={"data": overpass_query},
            headers={"Accept": "application/json"},
            timeout=30,
        )
        resp.raise_for_status()
        elements = resp.json().get("elements", [])
    except Exception as e:
        logger.warning(f"OSM query failed for '{query}' in {city}: {e}")
        return []

    results = []
    for el in elements:
        tags = el.get("tags", {})
        lat_val = el.get("lat") or el.get("center", {}).get("lat")
        lng_val = el.get("lon") or el.get("center", {}).get("lon")

        name = tags.get("name", "")
        if not name:
            continue

        addr_parts = [
            tags.get("addr:street", ""),
            tags.get("addr:city", city),
            tags.get("addr:postcode", ""),
        ]
        address = ", ".join(p for p in addr_parts if p)

        results.append({
            "source": "osm",
            "brand": tags.get("brand", extract_brand_from_title(name)),
            "title": name,
            "address": address,
            "city": city,
            "state": tags.get("addr:state"),
            "pincode": tags.get("addr:postcode"),
            "latitude": lat_val,
            "longitude": lng_val,
            "rating": None,
            "review_count": None,
            "phone": tags.get("phone") or tags.get("contact:phone"),
            "website": tags.get("website") or tags.get("contact:website"),
            "category": tags.get("cuisine") or tags.get("shop") or tags.get("amenity"),
            "reviews_text": None,
            "confidence": 0.6,
        })

    return results
