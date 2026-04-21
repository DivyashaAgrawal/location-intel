"""
Google Places API (v1) adapter.

This is the primary maps source. One `searchText` call returns up to 20 rows;
the adapter follows `nextPageToken` up to 3 pages (max 60 rows/city).
A full field mask is sent so a single call yields everything downstream
needs -- address components for state/pincode, rating, review count, phone,
website, categories.

Every successful call logs one row to `db.api_call_log` with the Google
Places unit cost. If `GOOGLE_PLACES_API_KEY` is unset the adapter returns
`[]` and the cache_manager falls through to Serper / OSM.
"""
from __future__ import annotations

import logging

import time
from typing import Optional

import requests

from src.core import db
from src.core.config import GOOGLE_PLACES_API_KEY, INDIA_MAJOR_CITIES
from src.fetchers._common import extract_pincode

logger = logging.getLogger(__name__)


SEARCH_URL = "https://places.googleapis.com/v1/places:searchText"

# Comma-separated FieldMask: everything we need, nothing we don't.
FIELD_MASK = (
    "places.id,"
    "places.displayName,"
    "places.formattedAddress,"
    "places.addressComponents,"
    "places.location,"
    "places.rating,"
    "places.userRatingCount,"
    "places.nationalPhoneNumber,"
    "places.websiteUri,"
    "places.primaryType,"
    "places.types,"
    "nextPageToken"
)

MAX_PAGES = 3
PAGE_SIZE = 20


def _component(components: list[dict], kind: str) -> Optional[str]:
    """Return the longText of the address component whose `types` contains `kind`."""
    for c in components or []:
        if kind in (c.get("types") or []):
            return c.get("longText") or c.get("shortText")
    return None


def _extract_pincode(address: str, components: list[dict]) -> Optional[str]:
    # Prefer the structured postal_code component; fall back to address regex.
    pc = _component(components, "postal_code")
    if pc:
        return str(pc)
    return extract_pincode(address or "")


def _normalize_place(place: dict, brand: str, city: str) -> dict:
    components = place.get("addressComponents") or []
    location = place.get("location") or {}
    title = (place.get("displayName") or {}).get("text") or ""
    address = place.get("formattedAddress") or ""

    return {
        "source": "google_places",
        "place_id": place.get("id"),
        "brand": brand,
        "title": title,
        "address": address,
        "city": _component(components, "locality") or city,
        "state": _component(components, "administrative_area_level_1"),
        "pincode": _extract_pincode(address, components),
        "latitude": location.get("latitude"),
        "longitude": location.get("longitude"),
        "rating": place.get("rating"),
        "review_count": place.get("userRatingCount"),
        "phone": place.get("nationalPhoneNumber"),
        "website": place.get("websiteUri"),
        "category": place.get("primaryType") or (place.get("types") or [None])[0],
        "reviews_text": None,
        "confidence": 0.95,
    }


def search_text(
    brand: str,
    city: str,
    api_key: Optional[str] = None,
    max_pages: int = MAX_PAGES,
    page_size: int = PAGE_SIZE,
    session: Optional[requests.Session] = None,
) -> list[dict]:
    """
    Search Google Places v1 for `"{brand} in {city}, India"`, paginating up to
    `max_pages`. Returns a list of normalised store dicts matching the
    multi_fetcher schema. Always logs to `api_call_log`.
    """
    key = api_key or GOOGLE_PLACES_API_KEY
    if not key:
        return []

    http = session or requests

    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": key,
        "X-Goog-FieldMask": FIELD_MASK,
    }

    # Light location bias if we know the city centre.
    location_bias = None
    if city in INDIA_MAJOR_CITIES:
        loc = INDIA_MAJOR_CITIES[city]
        location_bias = {
            "circle": {
                "center": {"latitude": loc["lat"], "longitude": loc["lng"]},
                "radius": 25_000.0,  # 25 km
            }
        }

    results: list[dict] = []
    page_token: Optional[str] = None

    for page in range(max_pages):
        body: dict = {
            "textQuery": f"{brand} in {city}, India",
            "pageSize": page_size,
        }
        if location_bias is not None:
            body["locationBias"] = location_bias
        if page_token:
            body["pageToken"] = page_token

        success = False
        try:
            resp = http.post(SEARCH_URL, headers=headers, json=body, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            success = True
        except Exception as e:
            # Log the failed call for cost/telemetry consistency, then bail.
            db.log_api_call("google_places", brand=brand, city=city, success=False, cost=0.0)
            logger.warning(f"    [google_places] error for '{brand}' in {city}: {e}")
            break
        finally:
            if success:
                db.log_api_call("google_places", brand=brand, city=city, success=True)

        for place in data.get("places", []) or []:
            results.append(_normalize_place(place, brand, city))

        page_token = data.get("nextPageToken")
        if not page_token:
            break
        # Google requires a short delay before using the next page token.
        time.sleep(2.0)

    return results
