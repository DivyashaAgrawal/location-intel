"""
Interactive review of the `discovered_competitors` table.

Lists every entry sorted by times_seen DESC and prompts for each:

  (c) confirm as real (sets manually_verified = 1)
  (n) flag as noise (deletes the row)
  (s) skip (leaves it for later review)
  (q) quit

Intended for periodic cleanup. Not hooked into any cron.

Usage:
    python src/scripts/review_competitors.py
"""
from __future__ import annotations

import logging
import sys
from datetime import datetime

from src.caching import db

logger = logging.getLogger(__name__)


def _fmt_ts(ts: float) -> str:
    try:
        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return "-"


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    rows = db.list_all_discovered_competitors()
    if not rows:
        print("No discovered competitors.")
        return 0

    print(f"{len(rows)} discovered competitor(s) to review.\n")

    for i, row in enumerate(rows, start=1):
        brand = row["brand"]
        print(f"[{i}/{len(rows)}] {brand}")
        print(f"  category:  {row['category']}")
        print(f"  times_seen: {row['times_seen']}")
        print(f"  first_seen: {_fmt_ts(row['first_seen'])}")
        print(f"  last_seen:  {_fmt_ts(row['last_seen'])}")
        print(f"  source:     {row['source']}")
        print(f"  verified:   {'yes' if row.get('manually_verified') else 'no'}")
        choice = input("  (c)onfirm, (n)oise, (s)kip, (q)uit: ").strip().lower()
        if choice == "c":
            db.verify_discovered_competitor(brand)
            print(f"  -> {brand} confirmed")
        elif choice == "n":
            db.delete_discovered_competitor(brand)
            print(f"  -> {brand} deleted")
        elif choice == "q":
            print("Aborted.")
            return 0
        print()

    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
