"""
Console-script launcher for the Streamlit UI.

Invoke from outside an existing Streamlit Runtime via the `location-intel`
console script installed by pyproject.toml, or `python -m src.cli`.
"""
from __future__ import annotations

import sys
from pathlib import Path


def main() -> None:
    import streamlit.web.cli as stcli

    ui_path = Path(__file__).resolve().parent / "app.py"
    if not ui_path.exists():
        raise FileNotFoundError(f"UI module not found at {ui_path}")

    sys.argv = ["streamlit", "run", str(ui_path)]
    sys.exit(stcli.main())


if __name__ == "__main__":
    main()
