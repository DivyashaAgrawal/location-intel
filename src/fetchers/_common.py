"""Shared helpers used by multiple fetcher adapters."""
from __future__ import annotations

import re
from typing import Optional


def extract_brand_from_title(title: str) -> str:
    """Extract brand/chain name from a Google Maps-style title."""
    for sep in [" - ", ", ", " | ", " @ "]:
        if sep in title:
            return title.split(sep)[0].strip()
    return (title or "").strip()


def extract_pincode(address: str) -> Optional[str]:
    """Extract the first 6-digit Indian pincode from an address string."""
    if not address:
        return None
    match = re.search(r"\b(\d{6})\b", address)
    return match.group(1) if match else None
