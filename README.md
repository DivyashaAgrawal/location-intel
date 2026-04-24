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
make run       # streamlit run src/ui/streamlit_app.py
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

Core modules, grouped by pipeline stage:

| Module | Role |
|---|---|
| `src/nlu/parser.py` | NL -> structured query (Ollama + rule-based fallback) |
| `src/nlu/brand_resolver.py` | Query phrase -> canonical brand via registry + FAISS |
| `src/nlu/brand_size.py` | Fast cheap store-count estimate (cache + headline + scrape + Places) |
| `src/nlu/guardrails.py` | Pre-fetch budget projection; blocks over-budget queries |
| `src/cache/manager.py` | `smart_fetch`: Redis -> DB -> API |
| `src/cache/db.py` | Persistent stores + query_cache + source_cache + api_call_log + brand_metadata + discovered_competitors |
| `src/cache/redis_cache.py` | Redis hot-cache wrapper |
| `src/config/settings.py` | Env + constants (API keys, tier-1 cities, thresholds) |
| `src/config/logging_setup.py` | Logging configuration |
| `src/fetchers/multi_fetcher.py` | Orchestrates adapters |
| `src/fetchers/google_places.py` | Primary maps source (Places v1) |
| `src/fetchers/serper.py` | Maps fallback |
| `src/fetchers/osm.py` | OpenStreetMap POI |
| `src/fetchers/brand_scraper.py` | Per-brand first-party scrapers |
| `src/fetchers/brand_scraper_js.py` | Playwright-backed scraper for JS-rendered locators |
| `src/reconciler/reconciler.py` | Cross-source dedup + field merge |
| `src/analysis/competitor.py` | Auto competitor + territory classification |
| `src/analysis/aggregator.py` | Pincode / city / state rollup |
| `src/analysis/market_analysis.py` | Density, whitespace, IC memo |
| `src/analysis/pincode_mapper.py` | Reverse geocoding to pincode |
| `src/analysis/sentiment.py` | Rating-derived sentiment |
| `src/pipeline.py` | End-to-end orchestrator |
| `src/cli.py` | Console-script launcher |
| `src/ui/streamlit_app.py` | Streamlit UI |
| `src/tools/` | End-user utilities installed as console scripts (`warm_cache`, `export_data`) |
| `src/maintenance/` | Developer-run scripts — one-off registry/index rebuilds, API discovery |

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
their store locators and parse the DOM. See `docs/BRAND_SCRAPER_STATUS.md` for
the per-brand status and selector-verification checklist.

To hunt for hidden JSON endpoints behind a JS locator and skip the
browser entirely, run `python src/maintenance/discover_apis.py`.

---

## 4a. Brand Recognition

The tool recognises 500+ Indian retail brands via a `brand_registry` table
backed by a FAISS(Facebook AI Similarity Search) nearest-neighbour index over sentence-transformer
embeddings. This solves the "multi-word brand names get misclassified as
categories" problem — "Biryani By Kilo" stays a brand; it isn't read as a
biryani category query.

### How it works

When a query arrives, the pipeline:

1. Extracts candidate phrases (stopwords, city names, and single-word
   category triggers like `pizza`/`coffee` are filtered out).
2. First tries an exact substring / alias match against the registry —
   fast path, confidence = high.
3. Falls back to a FAISS cosine search over normalized embeddings, with a
   token-overlap guardrail so `pizza` can't resolve to `Sbarro`.
4. Routes based on thresholds:
   - `score >= 0.92` → `confidence=high`, the NLU skips LLM brand
     classification entirely.
   - `score >= 0.75` → `confidence=ambiguous`, the hint is injected into
     the Ollama system prompt as a note.
   - Otherwise → `confidence=none`, the NLU runs as before.

### Growing the registry

The `brand_registry` table grows automatically:

- **Category queries** ("all pizza stores in Delhi") upsert discovered
  brand names with `source='discovered_category'`.
- **Scraper runs** confirm a brand exists and mark it `verified=1` with
  `source='discovered_scraper'`.
- **Manual additions**: edit `src/maintenance/build_seed_brands.py` to extend
  the curated list, then re-run:

  ```bash
  python src/maintenance/build_seed_brands.py   # writes data/brands_seed.csv
  python src/maintenance/load_brand_seed.py     # upserts into brand_registry
  python src/maintenance/rebuild_brand_index.py # rebuilds FAISS index
  ```

After 20+ new brands have accumulated since the last rebuild, the pipeline
logs a reminder. Index rebuilds are explicit (never automatic at query
time).

### Dependencies

Embedding-based resolution needs `sentence-transformers` and `faiss-cpu`
(adds ~500MB including torch). Install the extra:

```bash
pip install 'location-intel[embeddings]'
```

Without these packages the resolver falls back to exact substring /
alias matching against `brand_registry`. The pipeline still works; it's
just less tolerant of misspellings.

### Optional: HF_TOKEN for faster model downloads

The first time the resolver runs it pulls
`paraphrase-multilingual-MiniLM-L12-v2` (~500 MB) from the Hugging Face
Hub and caches it under `~/.cache/huggingface/`. Subsequent runs load
from disk and don't hit the Hub at all.

Anonymous downloads work fine but are rate-limited and slower. If you
rebuild the index often, pull new models, or share a public IP with
heavy HF usage, add a free token:

1. Create one at `https://huggingface.co/settings/tokens` (read scope).
2. Add to `.env`:

   ```
   HF_TOKEN=hf_xxx
   ```

`sentence-transformers` / `huggingface_hub` pick it up automatically.
The unauthenticated-hub warning is suppressed by default in
`src/nlu/brand_resolver.py`, so it's purely a speed consideration.

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

- Edit `KNOWN_BRANDS` in `src/nlu/parser.py` for NL recognition.
- Edit `BRAND_REGISTRY` in `src/fetchers/brand_scraper.py` with
  `store_locator_url`, `extraction_method`, `domain`, `last_verified`.
- If `extraction_method` is `html`, the shared Ollama HTML-to-JSON parser
  handles the rest.

**Extending the competitor map**

- Edit `COMPETITOR_MAP` in `src/analysis/competitor.py`.

**`src/tools/` vs `src/maintenance/`**

- `src/tools/` — user-facing operational utilities that ship with the package
  and are wired as console scripts in `pyproject.toml` (`warm-cache`,
  `export-data`). Use these during day-to-day operation.
- `src/maintenance/` — developer-run one-off scripts for registry and index
  maintenance (`build_seed_brands`, `load_brand_seed`, `rebuild_brand_index`,
  `refresh_brand_sizes`, `discover_apis`, `review_competitors`). Not
  installed as entry points; run directly with `python src/maintenance/<name>.py`.
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
SQLite DB lives at `data/location_intel.db` (override with
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
  [docs/BRAND_SCRAPER_STATUS.md](docs/BRAND_SCRAPER_STATUS.md) for per-brand status.
- **Playwright adds ~500 MB to the install footprint.** The `[playwright]`
  extra and `playwright install chromium` pull in a headless browser.
  Without it, JS-rendered brands fall through to Google Places.
- **Selectors for JS-rendered brands are best-guess** until manually
  verified against the live site. Run `src/maintenance/discover_apis.py` to find
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
