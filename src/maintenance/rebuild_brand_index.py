"""
Build the FAISS index for the brand registry.

Each brand is encoded as "<canonical_name> <alias1> <alias2> ... <category>"
and stored in an IndexFlatIP (inner product on normalized vectors == cosine).
Run explicitly whenever the registry grows (or use --only-new to rebuild from
scratch after N additions -- see Phase 5.4).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

INDEX_PATH = ROOT / "data" / "brand_index.faiss"
METADATA_PATH = ROOT / "data" / "brand_index_metadata.json"


def _corpus_text(brand: dict) -> str:
    parts = [brand["canonical_name"]]
    for a in brand.get("aliases") or []:
        if a:
            parts.append(a)
    if brand.get("category"):
        parts.append(brand["category"])
    return " ".join(parts)


def build_index(
    only_new: bool = False,
    index_path: Path = INDEX_PATH,
    metadata_path: Path = METADATA_PATH,
    db_path: str | None = None,
) -> dict:
    try:
        import faiss
        import numpy as np
        from sentence_transformers import SentenceTransformer
    except ImportError as e:
        print(
            f"Error: {e}\n"
            f"Install with: pip install 'location-intel[embeddings]'",
            file=sys.stderr,
        )
        raise

    from src.cache import db as _db
    from src.nlu.brand_resolver import EMBEDDING_MODEL

    brands = _db.list_all_brands_in_registry(db_path=db_path)
    if not brands:
        raise RuntimeError(
            "brand_registry is empty. Run `python src/scripts/load_brand_seed.py` first."
        )

    existing_names: set[str] = set()
    if only_new and metadata_path.exists():
        with metadata_path.open("r", encoding="utf-8") as f:
            prev = json.load(f)
        for b in prev.get("brands", []):
            existing_names.add(b["canonical_name"].lower())

    # Current code still embeds the full registry -- FAISS IndexFlatIP is dense
    # and can't merge incrementally. `--only-new` matters as a signal from 5.4
    # that a rebuild should happen; the rebuild itself is full. Document this.
    to_embed = brands

    texts = [_corpus_text(b) for b in to_embed]

    t0 = time.perf_counter()
    model = SentenceTransformer(EMBEDDING_MODEL)
    embeddings = model.encode(
        texts, normalize_embeddings=True, show_progress_bar=False
    )
    embeddings = np.asarray(embeddings, dtype="float32")
    dim = embeddings.shape[1]

    index = faiss.IndexFlatIP(dim)
    index.add(embeddings)

    index_path.parent.mkdir(parents=True, exist_ok=True)
    faiss.write_index(index, str(index_path))

    metadata = {
        "model": EMBEDDING_MODEL,
        "dimension": int(dim),
        "built_at": time.time(),
        "brands": [
            {
                "brand_id": b.get("brand_id"),
                "canonical_name": b["canonical_name"],
                "category": b.get("category"),
                "aliases": b.get("aliases", []),
            }
            for b in to_embed
        ],
    }
    metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2))

    dt = time.perf_counter() - t0
    index_size_kb = os.path.getsize(index_path) / 1024.0
    return {
        "brands_indexed": len(to_embed),
        "new_since_last": len(to_embed) - len(existing_names) if only_new else len(to_embed),
        "dimension": int(dim),
        "index_file_kb": round(index_size_kb, 1),
        "elapsed_sec": round(dt, 2),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Rebuild the FAISS brand index.")
    parser.add_argument(
        "--only-new",
        action="store_true",
        help="Hint: only triggered when delta rebuild is desired. "
             "Current impl always does a full rebuild but reports the delta.",
    )
    args = parser.parse_args()

    stats = build_index(only_new=args.only_new)
    print(
        f"Indexed {stats['brands_indexed']} brands "
        f"(dim={stats['dimension']}, "
        f"file={stats['index_file_kb']}KB, "
        f"elapsed={stats['elapsed_sec']}s)."
    )
    if args.only_new:
        print(f"New since last build: {stats['new_since_last']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
