# Changelog

## v0.2.1 - 2026-04-21

### Added

- **Playwright-based brand scraping.** New `src/fetchers/brand_scraper_js.py`
  renders JS store locators headlessly and parses the DOM. Three brands
  upgraded: Starbucks, KFC, Pizza Hut. Playwright is an optional extra
  (`pip install 'location-intel[playwright]'`); absence is fine.
- **Brand size estimation.** New `brand_metadata` table and
  `cache_manager.estimate_brand_size()` return a 90-day cached total
  store count with coverage %, driven by a headline extractor on the
  brand's locator page, a full scrape, or a Places-pagination fallback.
- **Query scope guardrails.** `MAX_ENRICHMENT_CALLS_PER_QUERY = 100`.
  Queries projected to exceed this are blocked with a message listing
  cities already in the DB and tier-1 city suggestions as clickable chips.
- **Persistent `discovered_competitors` registry.** Category queries
  auto-record novel brand names with a category tag. Subsequent
  competitor analyses merge these with the hand-curated map.
  `manually_verified` flag and UI curation in the sidebar.
- **Lazy Google Places enrichment.** New
  `smart_fetch_with_enrichment(brand, cities)` runs the brand scraper
  once nationally, then enriches only stores in queried cities.
  `enriched_at` / `enrichment_source` columns track freshness per store.
- **Scripts**: `scripts/discover_apis.py` (hunt JSON endpoints behind
  JS locators), `scripts/refresh_brand_sizes.py` (monthly cron),
  `scripts/review_competitors.py` (interactive CLI curation).
- **Test suite.** 35 tests covering Playwright graceful degradation,
  brand size estimation decision tree, query guardrails, lazy
  enrichment flow, and discovered-competitor lifecycle.

### Changed

- **Source priority reordered**: `brand_website -> google_places ->
  serper -> osm`. (Was already the default in `multi_fetcher`; now
  documented and enforced for the two-stage enrichment flow.)
- Dispatcher in `brand_scraper.py` now routes `extraction_method:
  "playwright"` to `brand_scraper_js`. `"js_rendered"` is a deprecated
  alias that auto-remaps with a warning.
- `stores` table: additive migration adds `enriched_at` and
  `enrichment_source` columns.

### Fixed

- Starbucks / KFC / Pizza Hut are no longer hard-skipped. With
  Playwright installed and selectors verified, they flow through the
  Playwright dispatcher. Without Playwright, the old graceful-degradation
  path (empty DataFrame, fall through to Google) still holds.
