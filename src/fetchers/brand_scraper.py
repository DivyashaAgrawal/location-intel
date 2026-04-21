"""
Brand Website Scraper: Extract store locations from brand websites.

Strategy per brand, in order of preference:
1. `api`        -- hit a known JSON endpoint directly (cheapest)
2. `html`       -- fetch HTML + Ollama to parse into structured data
3. `playwright` -- render the page headlessly, then parse the rendered DOM
                   (delegates to `brand_scraper_js`; requires playwright)
4. `blocked`    -- marked unreachable; adapter returns [] so multi-source
                   reconciliation silently falls through to other sources.

`js_rendered` is a deprecated alias for `playwright`; the dispatcher remaps
it automatically with a warning.
"""
from __future__ import annotations

import logging

import json
import random
import time
import requests
from bs4 import BeautifulSoup
import pandas as pd
from src.core.config import OLLAMA_BASE_URL, OLLAMA_MODEL
from src.fetchers._common import extract_pincode

logger = logging.getLogger(__name__)


BRAND_REGISTRY = {
    "Dominos Pizza": {
        "store_locator_url": "https://www.dominos.co.in/store-locator",
        "api_url": "https://www.dominos.co.in/api/store-locator/search?city={city}",
        "extraction_method": "blocked",
        "domain": "dominos.co.in",
        "notes": "Registered URL 404s as of 2026-04-20; see BRAND_SCRAPER_STATUS.md.",
        "last_verified": "2026-04-20",
    },
    "McDonald's": {
        "store_locator_url": "https://www.mcdonaldsindia.com/locate-us",
        "api_url": None,
        "extraction_method": "blocked",
        "domain": "mcdonaldsindia.com",
        "notes": "Registered locator URL 404s as of 2026-04-20; refresh needed.",
        "last_verified": "2026-04-20",
    },
    "Starbucks": {
        "store_locator_url": "https://www.starbucks.in/store-locator",
        "locator_url": "https://www.starbucks.in/store-locator",
        "api_url": None,
        "extraction_method": "playwright",
        "domain": "starbucks.in",
        "notes": (
            "JS-rendered. Selectors below are best-guess from public DOM "
            "patterns; verify on live site and adjust before production use."
        ),
        "last_verified": "2026-04-20",
        "wait_selector": ".store-result-item, .store-card, [data-testid='store-card']",
        "item_selector": ".store-result-item, .store-card, [data-testid='store-card']",
        "fields": {
            "title": ".store-name, h3, .name",
            "address": ".store-address, .address",
            "phone": ".store-phone, .phone",
            "city": ".store-city, .city",
        },
        "load_more_selector": ".load-more-btn, button:has-text('Load More')",
        "max_clicks": 50,
        "headline_count_selector": ".store-count, .total-stores, .result-count",
        "headline_count_regex": r"(\d[\d,]*)\s*stores?",
    },
    "Da Milano": {
        "store_locator_url": "https://www.damilano.com/pages/store-locator",
        "api_url": None,
        "extraction_method": "blocked",
        "domain": "damilano.com",
        "notes": "Registered locator URL 404s as of 2026-04-20; homepage is reachable.",
        "last_verified": "2026-04-20",
    },
    "Nykaa": {
        "store_locator_url": "https://www.nykaa.com/sp/store-locator/store-locator",
        "api_url": None,
        "extraction_method": "blocked",
        "domain": "nykaa.com",
        "notes": "WAF returns 403 even with hardened headers. Fall back to Serper.",
        "last_verified": "2026-04-20",
    },
    "Tanishq": {
        "store_locator_url": "https://www.tanishq.co.in/store-locator",
        "api_url": "https://www.tanishq.co.in/api/store/search?city={city}",
        "extraction_method": "blocked",
        "domain": "tanishq.co.in",
        "notes": "API and locator URLs both 404 as of 2026-04-20; homepage OK.",
        "last_verified": "2026-04-20",
    },
    "Lenskart": {
        "store_locator_url": "https://www.lenskart.com/stores",
        "api_url": "https://api.lenskart.com/v2/lkstore/stores?city={city}&pageSize=50",
        "extraction_method": "blocked",
        "domain": "lenskart.com",
        "notes": "API 404 and homepage returns Brotli-encoded response that decoder rejects.",
        "last_verified": "2026-04-20",
    },
    "FabIndia": {
        "store_locator_url": "https://www.fabindia.com/storelocator",
        "api_url": None,
        "extraction_method": "blocked",
        "domain": "fabindia.com",
        "notes": "Registered locator URL 404s; homepage reachable.",
        "last_verified": "2026-04-20",
    },
    "KFC": {
        "store_locator_url": "https://online.kfc.co.in/store-locator",
        "locator_url": "https://online.kfc.co.in/store-locator",
        "api_url": None,
        "extraction_method": "playwright",
        "domain": "kfc.co.in",
        "notes": (
            "JS-rendered. Selectors below are best-guess from public DOM "
            "patterns; verify on live site and adjust before production use."
        ),
        "last_verified": "2026-04-20",
        "wait_selector": ".store-card, .restaurant-card, [data-store-id]",
        "item_selector": ".store-card, .restaurant-card, [data-store-id]",
        "fields": {
            "title": ".store-name, .restaurant-name, h3",
            "address": ".store-address, .address, .location-address",
            "phone": ".store-phone, .phone",
            "city": ".store-city, .city",
        },
        "load_more_selector": "button:has-text('Show More'), .load-more",
        "max_clicks": 50,
        "headline_count_selector": ".store-count, .total-count, .results-count",
        "headline_count_regex": r"(\d[\d,]*)\s*(?:stores?|restaurants?|outlets?)",
    },
    "Pizza Hut": {
        "store_locator_url": "https://www.pizzahut.co.in/store-locator",
        "locator_url": "https://www.pizzahut.co.in/store-locator",
        "api_url": None,
        "extraction_method": "playwright",
        "domain": "pizzahut.co.in",
        "notes": (
            "JS-rendered. Same parent company as KFC (Yum Brands). Selectors "
            "below are best-guess; verify on live site before production use."
        ),
        "last_verified": "2026-04-20",
        "wait_selector": ".store-card, .outlet-card, [data-store-id]",
        "item_selector": ".store-card, .outlet-card, [data-store-id]",
        "fields": {
            "title": ".store-name, .outlet-name, h3",
            "address": ".store-address, .address",
            "phone": ".store-phone, .phone",
            "city": ".store-city, .city",
        },
        "load_more_selector": "button:has-text('Load More'), .load-more",
        "max_clicks": 50,
        "headline_count_selector": ".store-count, .total-count, .results-count",
        "headline_count_regex": r"(\d[\d,]*)\s*(?:stores?|outlets?)",
    },
    "Bata": {
        "store_locator_url": "https://www.bata.in/store-locator",
        "api_url": None,
        "extraction_method": "html",
        "domain": "bata.in",
        "notes": "Homepage times out in probe; left as html until re-verified.",
        "last_verified": "2026-04-20",
    },
    "Haldiram's": {
        "store_locator_url": "https://www.haldirams.com/store-locator",
        "api_url": None,
        "extraction_method": "html",
        "domain": "haldirams.com",
        "notes": "Working as of 2026-04-20. Returns ~282 KB HTML; parsed by Ollama step.",
        "last_verified": "2026-04-20",
    },
}

SCRAPER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,application/json;q=0.8,*/*;q=0.7",
    "Accept-Language": "en-IN,en-GB;q=0.9,en;q=0.8",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "Cache-Control": "max-age=0",
}


def get_brand_info(brand: str) -> dict | None:
    """Look up a brand in the registry. Fuzzy matches on name."""
    if brand in BRAND_REGISTRY:
        return BRAND_REGISTRY[brand]

    brand_lower = brand.lower().strip()
    for key, val in BRAND_REGISTRY.items():
        if brand_lower in key.lower() or key.lower() in brand_lower:
            return val

    return None


def scrape_brand_api(brand: str, api_url: str, cities: list[str]) -> list[dict]:
    """
    Try to hit the brand's store locator API directly.
    Many Indian retail brands have JSON APIs behind their store locators.

    Uses hardened headers + a Referer set to the brand's domain and a
    random 2-5 s jitter between cities to look less burst-like.
    """
    all_stores = []

    info = BRAND_REGISTRY.get(brand, {})
    domain = info.get("domain", "")
    headers = dict(SCRAPER_HEADERS)
    if domain:
        headers["Referer"] = f"https://www.{domain}/"

    for city in cities:
        url = api_url.format(city=city)
        try:
            resp = requests.get(url, headers=headers, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                stores = _extract_stores_from_json(data, brand, city)
                all_stores.extend(stores)
                logger.info(f"    Brand API: {len(stores)} stores in {city}")
            else:
                logger.info(f"    Brand API returned {resp.status_code} for {city}")
        except Exception as e:
            logger.warning(f"    Brand API error for {city}: {e}")

        time.sleep(random.uniform(2.0, 5.0))

    return all_stores


def _extract_stores_from_json(data: dict | list, brand: str, city: str) -> list[dict]:
    """
    Extract store records from various JSON structures.
    Brands use different schemas, so we try common patterns.
    """
    stores = []

    store_list = []
    if isinstance(data, list):
        store_list = data
    elif isinstance(data, dict):
        for key in ["stores", "results", "data", "storeList", "outlets", "locations", "items"]:
            if key in data and isinstance(data[key], list):
                store_list = data[key]
                break
        if not store_list:
            for v in data.values():
                if isinstance(v, dict):
                    for key in ["stores", "results", "data"]:
                        if key in v and isinstance(v[key], list):
                            store_list = v[key]
                            break

    for item in store_list:
        if not isinstance(item, dict):
            continue

        store = {
            "brand": brand,
            "source": "brand_website",
            "city": city,
            "title": _get_nested(item, ["name", "storeName", "store_name", "title", "outlet_name"]),
            "address": _get_nested(item, ["address", "full_address", "storeAddress", "formatted_address", "location"]),
            "pincode": _get_nested(item, ["pincode", "zipcode", "zip", "postalCode", "postal_code"]),
            "state": _get_nested(item, ["state", "stateName", "state_name"]),
            "phone": _get_nested(item, ["phone", "phoneNumber", "contact", "mobile", "tel"]),
            "latitude": _get_nested(item, ["latitude", "lat", "geo_lat"]),
            "longitude": _get_nested(item, ["longitude", "lng", "lon", "geo_lng"]),
        }

        if not store["pincode"] and store["address"]:
            store["pincode"] = extract_pincode(str(store["address"]))

        if store["title"] or store["address"]:
            stores.append(store)

    return stores


def _get_nested(d: dict, keys: list[str]):
    """Try multiple possible keys to extract a value from a dict."""
    for key in keys:
        if key in d and d[key] is not None:
            return d[key]
        for k, v in d.items():
            if k.lower() == key.lower() and v is not None:
                return v
    return None


def scrape_brand_html(brand: str, url: str, cities: list[str]) -> list[dict]:
    """
    Scrape store data from an HTML store locator page.
    Uses Ollama to parse the messy HTML into structured store records.
    """
    all_stores = []

    info = BRAND_REGISTRY.get(brand, {})
    domain = info.get("domain", "")
    headers = dict(SCRAPER_HEADERS)
    if domain:
        headers["Referer"] = f"https://www.{domain}/"

    try:
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code != 200:
            logger.info(f"    Brand page returned {resp.status_code}")
            return []

        html = resp.text
        soup = BeautifulSoup(html, "html.parser")

        for tag in soup(["script", "style", "nav", "footer", "header", "meta", "link", "noscript"]):
            tag.decompose()

        text = soup.get_text(separator="\n", strip=True)

        # Cap at 3000 chars to fit the 3B model context window.
        if len(text) > 3000:
            text = text[:3000]

        if not text.strip():
            logger.info(f"    No text content found on brand page")
            return []

        stores = _parse_stores_with_ollama(text, brand, cities)
        all_stores.extend(stores)

    except requests.ConnectionError:
        logger.info(f"    Could not connect to {url}")
    except Exception as e:
        logger.warning(f"    Brand HTML scraping error: {e}")

    return all_stores


def _parse_stores_with_ollama(text: str, brand: str, cities: list[str]) -> list[dict]:
    """Use Ollama to extract store records from raw page text."""
    cities_str = ", ".join(cities)

    prompt = f"""Extract store locations from this text for brand "{brand}".
Return ONLY a JSON array of stores. Each store object has: name, address, city, pincode, phone.
If no stores found, return [].
Focus on stores in these cities: {cities_str}

Text:
{text}

JSON array:"""

    try:
        resp = requests.post(
            f"{OLLAMA_BASE_URL}/api/generate",
            json={
                "model": OLLAMA_MODEL,
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": 0.1, "num_predict": 1000},
            },
            timeout=45,
        )
        resp.raise_for_status()

        raw = resp.json().get("response", "").strip()

        start = raw.find("[")
        end = raw.rfind("]") + 1
        if start >= 0 and end > start:
            store_list = json.loads(raw[start:end])

            return [
                {
                    "brand": brand,
                    "source": "brand_website",
                    "title": s.get("name", ""),
                    "address": s.get("address", ""),
                    "city": s.get("city", ""),
                    "pincode": str(s.get("pincode", "")),
                    "phone": s.get("phone", ""),
                    "state": s.get("state", ""),
                    "latitude": None,
                    "longitude": None,
                }
                for s in store_list
                if isinstance(s, dict)
            ]
    except requests.ConnectionError:
        logger.info("    Ollama not running for HTML parsing")
    except Exception as e:
        logger.warning(f"    Ollama HTML parsing error: {e}")

    return []


def scrape_brand_stores(
    brand: str,
    cities: list[str],
) -> pd.DataFrame:
    """
    Main entry point: scrape a brand's own website for store data.

    Returns a DataFrame with store records from the brand's website.
    Returns empty DataFrame if brand not in registry or scraping fails.
    """
    info = get_brand_info(brand)

    if info is None:
        logger.info(f"  Brand '{brand}' not in registry. Skipping website scraping.")
        return pd.DataFrame()

    method = info["extraction_method"]
    # `js_rendered` is the pre-Playwright spelling; treat identically.
    if method == "js_rendered":
        logger.info(f"  {brand}: remapping deprecated 'js_rendered' to 'playwright'")
        method = "playwright"

    logger.info(f"  Scraping {brand} website ({method})...")

    if method == "api" and info.get("api_url"):
        stores = scrape_brand_api(brand, info["api_url"], cities)
    elif method == "html":
        stores = scrape_brand_html(brand, info["store_locator_url"], cities)
    elif method == "playwright":
        from src.fetchers import brand_scraper_js
        if not brand_scraper_js.PLAYWRIGHT_AVAILABLE:
            logger.warning(
                f"    Playwright not installed; cannot scrape {brand}. "
                "Install with `pip install playwright && playwright install chromium`."
            )
            return pd.DataFrame()
        df = brand_scraper_js.scrape_with_playwright(brand, cities)
        if df.empty:
            logger.info(f"    No stores extracted from {brand} via Playwright")
        else:
            logger.info(f"    Extracted {len(df)} stores from {brand} via Playwright")
        return df
    elif method == "blocked":
        logger.warning(
            f"    {brand} is marked 'blocked' in the registry "
            f"(see BRAND_SCRAPER_STATUS.md). Skipping."
        )
        return pd.DataFrame()
    else:
        return pd.DataFrame()

    if not stores:
        logger.info(f"    No stores extracted from {brand} website")
        return pd.DataFrame()

    df = pd.DataFrame(stores)
    logger.info(f"    Extracted {len(df)} stores from {brand} website")
    return df
