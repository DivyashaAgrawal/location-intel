"""
Pincode mapper: Convert lat/lng coordinates to Indian pincodes and states.

Uses Nominatim (OpenStreetMap) for reverse geocoding.
Free, no API key needed, just needs a user agent string.
"""
from __future__ import annotations

import logging
import time

import pandas as pd
from geopy.exc import GeocoderTimedOut
from geopy.geocoders import Nominatim

from src.core.config import NOMINATIM_USER_AGENT
from src.fetchers._common import extract_pincode

logger = logging.getLogger(__name__)


STATE_ABBREVIATIONS = {
    "DL": "Delhi", "MH": "Maharashtra", "KA": "Karnataka",
    "TN": "Tamil Nadu", "TS": "Telangana", "AP": "Andhra Pradesh",
    "WB": "West Bengal", "GJ": "Gujarat", "RJ": "Rajasthan",
    "UP": "Uttar Pradesh", "MP": "Madhya Pradesh", "BR": "Bihar",
    "PB": "Punjab", "HR": "Haryana", "KL": "Kerala",
    "JH": "Jharkhand", "CG": "Chhattisgarh", "OR": "Odisha",
    "GA": "Goa", "UK": "Uttarakhand", "HP": "Himachal Pradesh",
    "AS": "Assam", "CH": "Chandigarh",
}




def reverse_geocode_to_pincode(
    lat: float,
    lng: float,
    geolocator: Nominatim | None = None,
) -> dict:
    """
    Reverse geocode coordinates to get pincode and state.
    Returns: {"pincode": str, "state": str, "district": str}
    """
    if geolocator is None:
        geolocator = Nominatim(user_agent=NOMINATIM_USER_AGENT)

    try:
        location = geolocator.reverse(
            f"{lat}, {lng}",
            exactly_one=True,
            language="en",
            timeout=10,
        )

        if location and location.raw.get("address"):
            addr = location.raw["address"]
            return {
                "pincode": addr.get("postcode", ""),
                "state": addr.get("state", ""),
                "district": addr.get("state_district", addr.get("county", "")),
            }
    except GeocoderTimedOut:
        pass
    except Exception as e:
        logger.warning(f"  Geocoding error for ({lat}, {lng}): {e}")

    return {"pincode": "", "state": "", "district": ""}


def enrich_with_pincodes(
    df: pd.DataFrame,
    delay: float = 1.1,  # Nominatim requires 1 req/sec
) -> pd.DataFrame:
    """
    Add pincode, state, and district columns to a DataFrame with lat/lng.

    If pincode is already extractable from the address field, skips the API call.
    Uses Nominatim (free) for reverse geocoding otherwise.
    """
    if df.empty:
        return df

    df = df.copy()

    if "address" in df.columns:
        df["pincode"] = df["address"].apply(
            lambda a: extract_pincode(str(a)) if pd.notna(a) else None
        )
    else:
        df["pincode"] = None

    needs_geocoding = df["pincode"].isna()

    if needs_geocoding.any():
        geolocator = Nominatim(user_agent=NOMINATIM_USER_AGENT)
        failed_geocodes = 0

        for idx in df[needs_geocoding].index:
            lat = df.at[idx, "latitude"]
            lng = df.at[idx, "longitude"]

            if pd.notna(lat) and pd.notna(lng):
                result = reverse_geocode_to_pincode(lat, lng, geolocator)
                if result["pincode"]:
                    df.at[idx, "pincode"] = result["pincode"]
                else:
                    failed_geocodes += 1

                if "state" not in df.columns or pd.isna(df.at[idx, "state"]):
                    df.at[idx, "state"] = result["state"]
                if "district" not in df.columns or pd.isna(df.at[idx, "district"]):
                    df.at[idx, "district"] = result["district"]

                # Nominatim rate limit is 1 req/sec.
                time.sleep(delay)

        if failed_geocodes > 0:
            logger.warning(f"  Warning: geocoding failed for {failed_geocodes} locations")

    for col in ["state", "district"]:
        if col not in df.columns:
            df[col] = ""

    return df
