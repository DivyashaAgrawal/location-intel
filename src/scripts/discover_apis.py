"""
Discover hidden JSON endpoints behind JS-rendered store locators.

Navigates each target brand's locator URL under Playwright with network
capture enabled, filters XHR/fetch responses that look like store-data
endpoints (path substrings: "store", "location", "outlet", "locate"),
and prints a report the operator can diff against the registry.

One-time optimisation. If a brand secretly fetches JSON, flipping the
registry entry from `extraction_method: "playwright"` to `"api"` (with the
discovered URL as `api_url`) avoids the browser-launch cost on every query.

Usage:
    python src/scripts/discover_apis.py
    python src/scripts/discover_apis.py --brand "Starbucks"
    python src/scripts/discover_apis.py --out discovery.json
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sys

from src.fetchers.brand_scraper import BRAND_REGISTRY

logger = logging.getLogger(__name__)

CANDIDATE_PATTERNS = [
    re.compile(r"/api/.*store", re.I),
    re.compile(r"/api/.*location", re.I),
    re.compile(r"/api/.*outlet", re.I),
    re.compile(r"store.*locator", re.I),
    re.compile(r"/get-?stores?", re.I),
    re.compile(r"/locate", re.I),
    re.compile(r"/outlets?", re.I),
]


def discover(brand: str, timeout_ms: int = 15_000) -> list[dict]:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print(
            "Playwright not installed. Run:\n"
            "  pip install 'location-intel[playwright]' && playwright install chromium"
        )
        sys.exit(1)

    info = BRAND_REGISTRY.get(brand)
    if not info:
        print(f"Unknown brand: {brand}")
        return []

    url = info.get("locator_url") or info.get("store_locator_url")
    if not url:
        print(f"{brand}: no locator URL configured in registry")
        return []

    captured: list[dict] = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()
        page.set_default_timeout(timeout_ms)

        def on_response(response):
            try:
                req = response.request
                if req.resource_type not in {"xhr", "fetch"}:
                    return
                url_str = req.url
                if not any(p.search(url_str) for p in CANDIDATE_PATTERNS):
                    return
                ct = (response.headers.get("content-type") or "").lower()
                if "json" not in ct:
                    return
                sample = None
                try:
                    sample = response.json()
                except Exception:
                    try:
                        sample = response.text()[:500]
                    except Exception:
                        sample = None
                captured.append({
                    "method": req.method,
                    "url": url_str,
                    "status": response.status,
                    "content_type": ct,
                    "post_data": req.post_data,
                    "sample": sample,
                })
            except Exception:
                pass

        page.on("response", on_response)
        try:
            page.goto(url, wait_until="networkidle", timeout=timeout_ms)
        except Exception as e:
            print(f"  {brand}: navigation error: {e}")
        finally:
            context.close()
            browser.close()

    return captured


def main() -> int:
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(
        description="Discover JSON endpoints on JS-rendered brand locators."
    )
    parser.add_argument(
        "--brand",
        help="Probe a single brand. Default: all playwright/js_rendered brands.",
    )
    parser.add_argument(
        "--out",
        default="-",
        help="Output JSON path, or '-' for stdout (default).",
    )
    args = parser.parse_args()

    if args.brand:
        targets = [args.brand]
    else:
        targets = [
            b for b, info in BRAND_REGISTRY.items()
            if info.get("extraction_method") in {"playwright", "js_rendered"}
        ]

    if not targets:
        print("No Playwright-backed brands in registry.")
        return 0

    report: dict[str, list[dict]] = {}
    for brand in targets:
        print(f"Probing {brand}...")
        report[brand] = discover(brand)
        print(f"  {len(report[brand])} candidate endpoint(s)")

    output = json.dumps(report, indent=2, default=str)
    if args.out == "-":
        print(output)
    else:
        with open(args.out, "w") as f:
            f.write(output)
        print(f"Wrote {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
