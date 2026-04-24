"""
Load ``data/brands_seed.csv`` into the ``brand_registry`` table.

Idempotent: re-running skips rows whose canonical_name already exists (the
registry's UNIQUE constraint plus the upsert helper's merge logic handles
aliases cleanly on repeated runs).
"""
from __future__ import annotations

import csv
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from src.cache import db as _db  # noqa: E402

SEED_PATH = ROOT / "data" / "brands_seed.csv"


def load_seed(csv_path: Path = SEED_PATH, db_path: str | None = None) -> dict[str, int]:
    """Load seed rows. Returns counts of inserted / updated / skipped rows."""
    if not csv_path.exists():
        raise FileNotFoundError(
            f"Seed CSV not found at {csv_path}. "
            f"Run `python src/scripts/build_seed_brands.py` first."
        )

    inserted = 0
    updated = 0
    skipped = 0

    with csv_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = (row.get("canonical_name") or "").strip()
            if not name:
                skipped += 1
                continue
            aliases_raw = (row.get("aliases") or "").strip()
            aliases = [a.strip() for a in aliases_raw.split(";") if a.strip()]
            category = (row.get("category") or "").strip() or None
            source = (row.get("source") or "seed").strip() or "seed"

            existed_before = _db.get_brand_from_registry(name, db_path=db_path) is not None
            _db.upsert_brand_to_registry(
                canonical_name=name,
                aliases=aliases,
                category=category,
                source=source,
                verified=0,
                db_path=db_path,
            )
            if existed_before:
                updated += 1
            else:
                inserted += 1

    return {"inserted": inserted, "updated": updated, "skipped": skipped}


def main() -> int:
    counts = load_seed()
    print(
        f"Loaded {counts['inserted']} new brand(s); "
        f"{counts['updated']} already existed (merged); "
        f"{counts['skipped']} skipped (blank name)."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
