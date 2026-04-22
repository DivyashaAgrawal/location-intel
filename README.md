# Location Intelligence

Natural-language location intelligence for retail brands.

---

## 1. Problem statement

- How many stores does a brand have, and where?
- Is it concentrated in one city, or spread across the country?
- Who are its competitors, and where do they overlap vs leave gaps?
- How do ratings and reviews compare across cities?
- Which cities are under-served relative to competitors?

Answering these manually is painful. Google Maps gives you one pin at a time,
paid data vendors don't drill down to the pincode level, and most people end
up stitching together spreadsheets store by store.

Location Intelligence answers these questions from a natural-language prompt.
It pulls data from brand websites, Google Places, Serper.dev, and
OpenStreetMap, reconciles it into a single store graph, caches everything
so repeat queries are free, auto-identifies competitors, and shows the
results in a simple Streamlit UI.

---

## 2. Quick start

**Prerequisites**: Python 3.10+. `make setup` handles Ollama + Redis for you.

```bash
git clone <repo-url>
cd location-intel
make setup     # venv, deps, Ollama + model, Redis install + start, DB init
make env       # opens .env in $EDITOR -- add GOOGLE_PLACES_API_KEY (SERPER_API_KEY optional)
make run       # streamlit run src/app.py
```

Run `make` on its own for the full list of commands.

Example query:

```
pincode wise Dominos stores in Delhi and Mumbai with ratings
```

Source attribution is visible in the sidebar expander; after the first run
the same query is served from Redis/DB at $0 cost.

---

## 3. Architecture

Full technical write-up in [docs/architecture.md](docs/architecture.md).

Core modules:

| Module | Role |
|---|---|
| `src/nlu.py` | NL -> structured query (Ollama + rule-based fallback) |
| `src/core/cache_manager.py` | `smart_fetch`: Redis -> DB -> API |
| `src/core/db.py` | Persistent stores + query_cache + source_cache + api_call_log |
| `src/fetchers/multi_fetcher.py` | Orchestrates adapters |
| `src/fetchers/google_places.py` | Primary maps source (Places v1) |
| `src/fetchers/serper.py` | Maps fallback |
| `src/fetchers/osm.py` | OpenStreetMap POI |
| `src/fetchers/brand_scraper.py` | Per-brand first-party scrapers |
| `src/analysis/reconciler.py` | Cross-source dedup + field merge |
| `src/analysis/competitor.py` | Auto competitor + territory classification |
| `src/analysis/aggregator.py` | Pincode / city / state rollup |
| `src/analysis/market_analysis.py` | Density, whitespace, IC memo |
| `src/app.py` | Streamlit UI |
| `src/pipeline.py` | End-to-end orchestrator |

---

## 4. Data sources

Priority order, highest to lowest. Earlier sources short-circuit later ones.

| Source | Role | Cost |
|---|---|---|
| Brand websites (HTTP) | Authoritative national footprint for brands with stable APIs | Free |
| Brand websites (Playwright) | Same, for JS-rendered locators (Starbucks, KFC, Pizza Hut) | Free |
| Google Places API v1 | Enrichment: ratings, phone, reviews for stores in queried cities | ~$0.032/call (post free tier) |
| Serper.dev Maps | Fallback when Google Places is empty for a city | Free tier: 2,500 queries; ~$0.003/call after |
| OpenStreetMap (Overpass) | Last-resort gap-fill | Free (rate-limited) |
| Nominatim (OSM) | Reverse geocoding lat/lng -> pincode | Free (1 req/sec) |
| Outscraper | Full review text (optional, stub) | ~$0.01/call |

Every outbound call is logged to `api_call_log` with an estimated cost, so
the sidebar shows cumulative spend in real time.

**Lazy enrichment.** A brand scraper runs once nationally per ~90 days
and writes all stores to the DB. Google Places then enriches *only*
stores in cities the analyst has queried - rating/phone/review_count
get stamped with `enriched_at`. Stores in un-queried cities stay in the
DB waiting. Cost scales linearly with actual analyst usage rather than
with national store counts.

**Query guardrail.** Queries whose projected enrichment exceeds
`MAX_ENRICHMENT_CALLS_PER_QUERY` (100) are blocked with a city-level
suggestion. This prevents accidental "all India" scans and keeps each
query inside the Places free tier.

### Playwright setup

Playwright is optional. Without it, JS-rendered brands fall through to
Google Places. To enable:

```bash
pip install 'location-intel[playwright]'
playwright install chromium          # ~500 MB browser binary
```

Once installed, `extraction_method: "playwright"` entries in the brand
registry (Starbucks, KFC, Pizza Hut) run headless Chromium to render
their store locators and parse the DOM. See `BRAND_SCRAPER_STATUS.md` for
the per-brand status and selector-verification checklist.

To hunt for hidden JSON endpoints behind a JS locator and skip the
browser entirely, run `python scripts/discover_apis.py`.

---

## 5. Caching

Cache hit rates in practice:

- Redis TTL 24h + DB TTL 24h means a query re-run within a day is free.
- OSM cached for 30 days (POIs don't move).
- Brand-website results cached 21 days.

The free tier on Serper (2,500 calls) covers several months of normal use
for a small team even before Places billing kicks in. Ollama is local so NLU
is free. Nominatim is free but rate-limited to 1 req/sec — pre-warm with
`python -m src.tools.warm_cache` before a heavy session.

---

## 6. Development guide

Runtime deps live in `[project.dependencies]` in `pyproject.toml`; lint
tooling (ruff, mypy) lives in `[project.optional-dependencies].dev`. Install
both with `make setup`, `make install`.

```bash
make lint
make type
```

**Adding a new data source adapter**

1. Create `src/fetchers/<new_source>.py` with a
   `search(brand, city) -> list[dict]` entry point returning records in the
   shared schema (see `multi_fetcher` module docstring).
2. Register it in `multi_fetcher.source_adapters`.
3. Add a priority row to `reconciler.SOURCE_PRIORITY` per field.
4. Add a TTL entry to `redis_cache.SOURCE_TTLS`.
5. Log every call with `db.log_api_call(source=...)`.

**Adding a new brand to the registry**

- Edit `KNOWN_BRANDS` in `src/nlu.py` for NL recognition.
- Edit `BRAND_REGISTRY` in `src/fetchers/brand_scraper.py` with
  `store_locator_url`, `extraction_method`, `domain`, `last_verified`.
- If `extraction_method` is `html`, the shared Ollama HTML-to-JSON parser
  handles the rest.

**Extending the competitor map**

- Edit `COMPETITOR_MAP` in `src/analysis/competitor.py`.
- Each entry is `brand -> [direct_competitors]`, max 5 per brand.

---

## 7. Example queries

- `pincode wise Dominos stores in Delhi and Mumbai with ratings`
- `pincode wise bikanervala in delhi` (unknown-to-registry brand; falls back to Google Places + OSM)
- `summary of dominos vs pizzahut in delhi and mumbai`
- `compare Haldirams vs McDonald's in Delhi, Mumbai, Bangalore`
- `state wise Da Milano locations with sentiment`
- `all pizza stores in bangalore` (category query; competitor tab skipped)

---

## 8. Deployment guide

Local dev only for now. Runs under `make run`.
SQLite DB lives at `location_intel.db` (override with
`LOCATION_INTEL_DB_PATH`). Redis is optional (`REDIS_URL` in `.env`) and
falls back to SQLite automatically.

Container/Cloud deployment (Docker, Cloud Run, etc.) is deferred until the
project moves out of the prototype phase -- the runtime is a single
Streamlit process, so containerising later is straightforward.

**Operational tools**

```bash
# Pre-populate the cache for an upcoming IC
python -m src.tools.warm_cache --brands "Dominos Pizza,McDonald's" --cities "Delhi,Mumbai"
# or:  make warm BRANDS="Dominos Pizza" CITIES="Delhi"

# Export for an external BI tool
python -m src.tools.export_data --format csv --output /tmp/stores.csv
# or:  make export FORMAT=csv OUT=/tmp/stores.csv
```

---

## 9. Limitations

- **Brand-scraper registry is 12 brands.** Anything outside the registry
  uses Google Places + OSM; the reconciler produces good output but
  addresses are less clean than a first-party source. See
  [BRAND_SCRAPER_STATUS.md](BRAND_SCRAPER_STATUS.md) for per-brand status.
- **Playwright adds ~500 MB to the install footprint.** The `[playwright]`
  extra and `playwright install chromium` pull in a headless browser.
  Without it, JS-rendered brands fall through to Google Places.
- **Selectors for JS-rendered brands are best-guess** until manually
  verified against the live site. Run `scripts/discover_apis.py` to find
  hidden JSON endpoints or inspect the rendered DOM to update selectors.
- **No "all India" scans.** Queries projected to exceed 100 Google Places
  enrichment calls are blocked with a city-level suggestion.
- **Serper pagination caps at ~60 stores/city.** Google Places does as
  well per search but can be refined by neighbourhood.
- **Sentiment is rating-based only.** Actual review text isn't pulled from
  Google Places (API limitation); Outscraper adapter is a stub.
- **Competitor map is hand-curated.** 20+ brands covered; unknown brands
  produce an empty competitor tab (not an error).
- **Data freshness bounded by per-source TTLs** (1-30 days). `invalidate`
  forces a fresh fetch.

---

## 10. Roadmap

**v2 candidates**:

- Playwright-backed scrapers for JS-rendered brand sites.
- Outscraper integration to pull full review text; LLM-based theme
  extraction to feed the sentiment pass.
- Auto-expand the brand registry by scraping India Retail Report catalogs.
- Scheduled warm-cache jobs via cron + `src.tools.warm_cache`
  for known-demand brands.
- Per-user query history persisted to DB (currently session-local).
- Parquet export alongside CSV/Excel for analyst pipelines.

---

## 11. License + credits

MIT License. See [LICENSE](LICENSE).

Built on open infrastructure: Streamlit, pandas, SQLite, Redis, Ollama,
OpenStreetMap / Nominatim / Overpass. Commercial data via Google Places
and Serper.dev. Brand-website scrapers respect each site's robots.txt and
ToS; the registry in `brand_scraper.py` is public-information only.

Questions / bug reports: open an issue on the repo or ping the tools team.
