# Brand Scraper Status

Last verified: **2026-04-20** (HTTP probes)
Playwright upgrade landed: **2026-04-21**

## Extraction methods

| Method        | Transport                                    | When to use                               |
|---------------|----------------------------------------------|-------------------------------------------|
| `api`         | Plain HTTPS request to a JSON endpoint       | Cheapest. Always preferred when available.|
| `html`        | Plain HTTPS + Ollama to parse the HTML       | Static HTML locators.                     |
| `playwright`  | Headless Chromium + DOM extraction           | JS-rendered locators. Requires Playwright.|
| `blocked`     | Adapter returns []                           | WAF / persistent 4xx / stale registry URL.|
| `js_rendered` | Deprecated alias for `playwright`            | Auto-remapped by the dispatcher.          |

Probe methodology (HTTP tier): GET with a realistic desktop Chrome header
set (User-Agent, Accept, Accept-Language, Sec-Fetch-*, Upgrade-Insecure-Requests,
Referer). Timeout 10s.

## Current registry status

| Brand         | Method        | URL status           | Stores extracted (Delhi test) | Last verified | Notes |
|---------------|---------------|----------------------|-------------------------------|---------------|-------|
| Dominos Pizza | blocked       | 404 (HTTP)           | 0                             | 2026-04-20    | Registered API + HTML paths both 404. Refresh URL before re-enabling. |
| McDonald's    | blocked       | 404 (HTTP)           | 0                             | 2026-04-20    | `/locate-us` drifted; homepage reachable. |
| Starbucks     | playwright    | pending live verify  | pending                       | 2026-04-20    | Registry carries best-guess selectors; re-check with `scripts/discover_apis.py`. |
| Da Milano     | blocked       | 404 (HTTP)           | 0                             | 2026-04-20    | `/pages/store-locator` returns 404. |
| Nykaa         | blocked       | 403 (HTTP)           | 0                             | 2026-04-20    | Cloudflare WAF; needs residential proxy. |
| Tanishq       | blocked       | 404 (HTTP)           | 0                             | 2026-04-20    | API + HTML locator both 404. |
| Lenskart      | blocked       | 404 + brotli         | 0                             | 2026-04-20    | Brotli content-encoding rejected. |
| FabIndia      | blocked       | 404 (HTTP)           | 0                             | 2026-04-20    | `/storelocator` drifted. |
| KFC           | playwright    | pending live verify  | pending                       | 2026-04-20    | Best-guess selectors; Yum Brands (same owner as Pizza Hut). |
| Pizza Hut     | playwright    | pending live verify  | pending                       | 2026-04-20    | Best-guess selectors. |
| Bata          | html          | TIMEOUT              | 0                             | 2026-04-20    | Intermittent socket timeout; may clear on a retry. |
| Haldiram's    | html          | 200                  | parse quality TBD             | 2026-04-20    | Only consistently-working HTML entry. Ollama parses the 282 KB page. |

## Playwright entries: selector verification required

Starbucks, KFC, and Pizza Hut now carry Playwright-specific selectors
(`wait_selector`, `item_selector`, per-field CSS selectors, `load_more_selector`,
`max_clicks`, `headline_count_selector`, `headline_count_regex`).

These selectors are best-guess, derived from public-DOM patterns. Before
relying on these brands in production, run:

```
playwright install chromium                             # one-time
python scripts/discover_apis.py                         # find hidden JSON APIs
# If no JSON endpoint is revealed, inspect the live DOM and update the
# selectors in src/fetchers/brand_scraper.py::BRAND_REGISTRY.
```

If `discover_apis.py` reveals a usable JSON endpoint, flip
`extraction_method` to `"api"` and populate `api_url`. That removes the
browser-launch overhead from every query.

## Graceful degradation

If Playwright is not installed:

- `brand_scraper_js.PLAYWRIGHT_AVAILABLE` is `False`.
- The dispatcher in `brand_scraper.scrape_brand_stores` logs a warning and
  returns an empty DataFrame for `playwright`-method brands.
- `multi_fetcher._fetch_brand_website` returns `[]`, and the reconciler
  silently falls through to Google Places / Serper / OSM.
- Nothing crashes.

## Per-brand recommended next steps

- **Dominos Pizza / McDonald's / Tanishq / Lenskart / Da Milano / FabIndia** -
  registered URLs are stale. Refresh each manually: visit the brand homepage,
  locate the current "Find a store" link, update `BRAND_REGISTRY`. Until then
  they remain `blocked`.
- **Nykaa** - WAF actively refuses us. Options: (a) residential proxy, (b)
  accept Google Places as primary for this brand. Stays `blocked`.
- **Bata** - socket timeout looks transient. Worth a retry before marking
  blocked. Left as `html`; adapter returns [] on timeout.
- **Haldiram's** - the only consistently-working HTML entry. Parse quality
  depends on the brand-agnostic Ollama step.
- **Starbucks / KFC / Pizza Hut** - now on the Playwright path; selectors
  need one live verification pass before production use.

## Future: automated status snapshot

A `tools/brand_scraper_probe.py` could re-run these probes on a cron and
keep this file current. Out of scope for now.
