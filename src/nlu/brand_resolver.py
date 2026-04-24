"""
Brand resolver: maps user query phrases to canonical brand names via embeddings.

The resolver is the single source of truth for "what brands exist." It replaces
hardcoded KNOWN_BRANDS dicts and hard-coded brand substring checks in the NLU.

Pipeline:
  1. Extract candidate phrases from user query (stopwords + geography filtered)
  2. Embed each candidate with a multilingual sentence-transformer
  3. FAISS nearest-neighbour search against the brand_registry embedding index
  4. Return the top match with a confidence bucket

If sentence-transformers / faiss are not installed, the module falls back to
exact substring matching against the brand_registry table. Callers always see
the same return shape.
"""
from __future__ import annotations

import contextlib
import json
import logging
import os
import sys
import threading
import warnings
from pathlib import Path
from typing import Any

# Silence the "sending unauthenticated requests to the HF Hub" warning. The
# sentence-transformers model is a small public checkpoint and is cached on
# disk after the first download, so anonymous rate limits are harmless here.
# Users who want the faster authenticated download can set HF_TOKEN in .env;
# huggingface_hub picks it up and this flag plus the silencing below become
# no-ops on their own.
os.environ.setdefault("HF_HUB_DISABLE_IMPLICIT_TOKEN", "1")
warnings.filterwarnings(
    "ignore", message=r".*unauthenticated requests to the HF Hub.*"
)
logging.getLogger("huggingface_hub").setLevel(logging.ERROR)


@contextlib.contextmanager
def _suppress_native_stderr():
    """
    Temporarily redirect OS-level stderr to /dev/null.

    Needed because the HF `hf_xet` downloader prints its unauthenticated-
    request warning from native Rust code, which bypasses Python's warnings
    and logging systems. Python exceptions still propagate normally -- they
    use their own path, not raw stderr writes -- so real failures during
    model load are unaffected.
    """
    try:
        fd = sys.stderr.fileno()
    except (AttributeError, OSError, ValueError):
        yield
        return
    try:
        sys.stderr.flush()
    except Exception:
        pass
    saved_fd = os.dup(fd)
    devnull_fd = os.open(os.devnull, os.O_WRONLY)
    try:
        os.dup2(devnull_fd, fd)
        yield
    finally:
        os.dup2(saved_fd, fd)
        os.close(devnull_fd)
        os.close(saved_fd)


logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[1]

EMBEDDING_MODEL = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
INDEX_PATH = ROOT / "data" / "brand_index.faiss"
METADATA_PATH = ROOT / "data" / "brand_index_metadata.json"

HIGH_CONFIDENCE_THRESHOLD = 0.92
AMBIGUOUS_THRESHOLD = 0.75

STOPWORDS: set[str] = {
    "get", "me", "show", "tell", "find", "give", "pincode", "city", "wise",
    "details", "list", "summary", "information", "data", "on", "in", "for",
    "of", "the", "a", "an", "stores", "locations", "outlets", "branches",
    "store", "location", "outlet", "branch", "please", "want", "need",
    "all", "across", "india", "near", "nearby", "count", "number",
    "rating", "ratings", "review", "reviews", "map", "maps",
    "delhi", "mumbai", "bangalore", "bengaluru", "chennai", "hyderabad",
    "pune", "kolkata", "ahmedabad", "surat", "jaipur", "lucknow", "kanpur",
    "nagpur", "indore", "bhopal", "patna", "vadodara", "agra", "nashik",
    "visakhapatnam", "gurgaon", "noida", "kochi", "chandigarh",
    "coimbatore", "trivandrum", "thiruvananthapuram",
    "vs", "versus", "compare", "against", "and", "or", "with",
    "ke", "mein",
}

# Single-word category triggers that should never, on their own, resolve to a
# specific brand via embeddings. Multi-word phrases containing these are still
# evaluated ("third wave coffee" is fine; bare "coffee" is not).
CATEGORY_SINGLE_WORDS: set[str] = {
    "pizza", "burger", "burgers", "coffee", "tea", "chai", "biryani", "bakery",
    "cafe", "salon", "bookstore", "pharmacy", "grocery", "jewelry", "jewellery",
    "apparel", "clothing", "eyewear", "footwear", "shoe", "shoes", "bank",
    "gym", "hotel", "restaurant", "restaurants", "ice", "cream",
    "sandwich", "chinese", "sweets", "sweet", "pet", "diagnostic",
    "electronics", "hospital", "beauty", "fitness",
}

MIN_PHRASE_CHARS = 3
MAX_NGRAM = 4
_PUNCT = ".,!?\"'()[]{}:;"


_model = None
_model_lock = threading.Lock()
_index = None
_index_metadata: list[dict[str, Any]] | None = None
_embeddings_available: bool | None = None


def _check_embeddings_available() -> bool:
    global _embeddings_available
    if _embeddings_available is not None:
        return _embeddings_available
    try:
        import faiss  # noqa: F401
        import sentence_transformers  # noqa: F401
        _embeddings_available = True
    except ImportError:
        logger.warning(
            "sentence-transformers / faiss not installed. "
            "Brand resolver will fall back to substring matching."
        )
        _embeddings_available = False
    return _embeddings_available


def _get_model():
    """Lazy-load the sentence-transformer. Cached as a global singleton."""
    global _model
    if _model is not None:
        return _model
    with _model_lock:
        if _model is None:
            from sentence_transformers import SentenceTransformer
            logger.info(f"Loading embedding model {EMBEDDING_MODEL}...")
            # hf_xet prints an unauthenticated-hub warning from native Rust
            # during the first download; suppress it so logs stay clean.
            # Real errors raise exceptions and are unaffected.
            with _suppress_native_stderr():
                _model = SentenceTransformer(EMBEDDING_MODEL)
    return _model


def _load_index() -> tuple[Any, list[dict[str, Any]]] | tuple[None, None]:
    """Load FAISS index + metadata from disk. Returns (None, None) if missing."""
    global _index, _index_metadata
    if _index is not None and _index_metadata is not None:
        return _index, _index_metadata
    if not INDEX_PATH.exists() or not METADATA_PATH.exists():
        logger.warning(
            f"Brand index not found at {INDEX_PATH}. "
            f"Run `python src/scripts/rebuild_brand_index.py` to build it."
        )
        return None, None
    import faiss
    _index = faiss.read_index(str(INDEX_PATH))
    with METADATA_PATH.open("r", encoding="utf-8") as f:
        payload = json.load(f)
    _index_metadata = payload.get("brands", [])
    return _index, _index_metadata


def extract_candidate_phrases(query: str) -> list[str]:
    """
    Build overlapping n-gram candidates after stripping stopwords + geography.
    Longer phrases come first so they win ties against their own sub-spans.
    """
    if not query:
        return []
    raw = query.lower()
    tokens = raw.split()
    meaningful: list[str] = []
    for tok in tokens:
        cleaned = tok.strip(_PUNCT)
        if not cleaned:
            continue
        if cleaned in STOPWORDS:
            continue
        meaningful.append(cleaned)

    if not meaningful:
        return []

    max_n = min(MAX_NGRAM, len(meaningful))
    candidates: list[str] = []
    for n in range(max_n, 0, -1):
        for i in range(len(meaningful) - n + 1):
            phrase = " ".join(meaningful[i:i + n])
            if len(phrase) >= MIN_PHRASE_CHARS:
                candidates.append(phrase)

    seen: set[str] = set()
    unique: list[str] = []
    for c in candidates:
        if c not in seen:
            seen.add(c)
            unique.append(c)
    return unique


def _empty_result() -> dict[str, Any]:
    return {
        "match_found": False,
        "confidence": "none",
        "canonical_brand": None,
        "score": 0.0,
        "candidate_phrase": None,
        "all_candidates": [],
        "method": "none",
    }


def _find_substring_match(
    raw_query: str, db_path: str | None = None
) -> tuple[str, str] | None:
    """Return (matched_phrase_lower, canonical_name) for the longest exact match."""
    from src.cache import db as _db

    raw_lower = raw_query.lower()
    try:
        brands = _db.list_all_brands_in_registry(db_path=db_path)
    except Exception as e:
        logger.warning(f"registry lookup failed: {e}")
        return None

    best: tuple[str, str] | None = None
    for brand in brands:
        canonical = brand["canonical_name"]
        aliases = brand.get("aliases", [])
        for name in [canonical] + list(aliases):
            if not name:
                continue
            n = name.lower()
            if len(n) < MIN_PHRASE_CHARS:
                continue
            # Word-boundary match: don't let "max" match "maximum" or "mall".
            # Use split() so "BBK" finds word "bbk" but not "bbkplus".
            if n in raw_lower and _is_word_boundary_match(n, raw_lower):
                if best is None or len(n) > len(best[0]):
                    best = (n, canonical)
    return best


def _is_word_boundary_match(needle: str, haystack: str) -> bool:
    """Check that needle appears in haystack only at token boundaries."""
    import re
    pattern = r"(?<![a-z0-9])" + re.escape(needle) + r"(?![a-z0-9])"
    return re.search(pattern, haystack) is not None


def resolve_query_fallback(raw_query: str, db_path: str | None = None) -> dict[str, Any]:
    """Exact substring match against the brand_registry. Used when embeddings absent."""
    if not raw_query or not raw_query.strip():
        return _empty_result()

    match = _find_substring_match(raw_query, db_path=db_path)
    if match is None:
        result = _empty_result()
        result["method"] = "substring_fallback"
        return result

    phrase, canonical = match
    return {
        "match_found": True,
        "confidence": "high",
        "canonical_brand": canonical,
        "score": 1.0,
        "candidate_phrase": phrase,
        "all_candidates": [(phrase, 1.0, {"canonical_name": canonical})],
        "method": "substring_fallback",
    }


def _candidates_for_embedding(raw_query: str) -> list[str]:
    """Candidates minus pure single-word category triggers (e.g. bare 'pizza')."""
    raw_candidates = extract_candidate_phrases(raw_query)
    keep: list[str] = []
    for c in raw_candidates:
        words = c.split()
        if len(words) == 1 and words[0] in CATEGORY_SINGLE_WORDS:
            continue
        keep.append(c)
    return keep


def _phrase_overlaps_canonical(phrase: str, canonical: str) -> bool:
    """Guardrail: require the matched phrase to share at least one token with
    the canonical name. Keeps 'pizza' from ever resolving to 'Sbarro'."""
    p_tokens = {t for t in phrase.lower().split() if len(t) >= 3}
    c_tokens = {t for t in canonical.lower().split() if len(t) >= 3}
    return bool(p_tokens & c_tokens)


def resolve_query(raw_query: str, db_path: str | None = None) -> dict[str, Any]:
    """
    Main entry point. Given a raw user query, identify if any brand is mentioned.

    Strategy:
      1. Exact substring / alias match against brand_registry -> confidence=high.
      2. Embedding search for fuzzy/misspelled hits, with guardrails that
         require token overlap with the canonical name and reject lone
         category words like 'pizza'.

    Returns a dict with keys: match_found, confidence (high/ambiguous/none),
    canonical_brand, score, candidate_phrase, all_candidates, method.
    """
    if not raw_query or not raw_query.strip():
        return _empty_result()

    exact = _find_substring_match(raw_query, db_path=db_path)
    if exact is not None:
        phrase, canonical = exact
        return {
            "match_found": True,
            "confidence": "high",
            "canonical_brand": canonical,
            "score": 1.0,
            "candidate_phrase": phrase,
            "all_candidates": [(phrase, 1.0, {"canonical_name": canonical})],
            "method": "substring",
        }

    if not _check_embeddings_available():
        return resolve_query_fallback(raw_query, db_path=db_path)

    index, metadata = _load_index()
    if index is None or not metadata:
        logger.info("No index available; using substring fallback.")
        return resolve_query_fallback(raw_query, db_path=db_path)

    candidates = _candidates_for_embedding(raw_query)
    if not candidates:
        return _empty_result()

    try:
        model = _get_model()
        embeddings = model.encode(
            candidates, normalize_embeddings=True, show_progress_bar=False
        )
    except Exception as e:
        logger.warning(f"embedding failed ({e}); falling back to substring match.")
        return resolve_query_fallback(raw_query, db_path=db_path)

    import numpy as np

    emb_matrix = np.asarray(embeddings, dtype="float32")
    if emb_matrix.ndim == 1:
        emb_matrix = emb_matrix.reshape(1, -1)

    k = min(5, len(metadata))
    scores, indices = index.search(emb_matrix, k)

    all_matches: list[tuple[str, float, dict[str, Any]]] = []
    for phrase_i, phrase in enumerate(candidates):
        for j in range(k):
            idx = int(indices[phrase_i][j])
            if idx < 0 or idx >= len(metadata):
                continue
            score = float(scores[phrase_i][j])
            all_matches.append((phrase, score, metadata[idx]))

    if not all_matches:
        return _empty_result()

    all_matches.sort(key=lambda x: x[1], reverse=True)

    # Pick the best match that passes the token-overlap guardrail.
    # The guardrail requires the matched phrase to share at least one token
    # with the canonical name. If nothing passes, the query likely refers to
    # a brand not yet in the index — return empty so Ollama / the rule-based
    # fallback can handle it rather than confidently returning the wrong brand.
    best: tuple[str, float, dict[str, Any]] | None = None
    for phrase, score, meta in all_matches:
        canonical = meta.get("canonical_name") or ""
        if _phrase_overlaps_canonical(phrase, canonical):
            best = (phrase, score, meta)
            break
    if best is None:
        return _empty_result()

    phrase, score, brand_meta = best
    canonical = brand_meta.get("canonical_name")

    if score >= HIGH_CONFIDENCE_THRESHOLD:
        confidence = "high"
    elif score >= AMBIGUOUS_THRESHOLD:
        confidence = "ambiguous"
    else:
        confidence = "none"

    return {
        "match_found": confidence != "none",
        "confidence": confidence,
        "canonical_brand": canonical if confidence != "none" else None,
        "score": score,
        "candidate_phrase": phrase,
        "all_candidates": [
            (p, s, {"canonical_name": m.get("canonical_name")})
            for p, s, m in all_matches[:5]
        ],
        "method": "embeddings",
    }


def reset_caches() -> None:
    """Clear cached model/index/metadata. Mostly for tests."""
    global _model, _index, _index_metadata, _embeddings_available
    _model = None
    _index = None
    _index_metadata = None
    _embeddings_available = None
