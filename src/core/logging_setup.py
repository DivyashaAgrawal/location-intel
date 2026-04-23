"""
Never log API keys, user queries that could contain PII, or full response
bodies -- the handlers don't filter for you.
"""
from __future__ import annotations

import logging
import logging.handlers
import os
from pathlib import Path

_LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"

_configured = False


def configure(log_dir: str | os.PathLike | None = None) -> None:
    """Idempotent: second and later calls are no-ops."""
    global _configured
    if _configured:
        return

    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    root = logging.getLogger()
    root.setLevel(level)
    # Clear any handlers a Streamlit/Jupyter host may have attached.
    for h in list(root.handlers):
        root.removeHandler(h)

    fmt = logging.Formatter(_LOG_FORMAT)

    console = logging.StreamHandler()
    console.setFormatter(fmt)
    console.setLevel(level)
    root.addHandler(console)

    log_dir = Path(log_dir) if log_dir else Path(
        os.environ.get("LOG_DIR", "logs")
    )
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
        file_handler = logging.handlers.RotatingFileHandler(
            log_dir / "app.log",
            maxBytes=10 * 1024 * 1024,   # 10 MB
            backupCount=5,
            encoding="utf-8",
        )
        file_handler.setFormatter(fmt)
        file_handler.setLevel(level)
        root.addHandler(file_handler)
    except OSError as e:
        # Read-only FS (e.g. some CI runners) -- console-only is fine.
        root.warning("File logging disabled (%s); using console only.", e)

    # Quiet the noisy third-parties down one level.
    for noisy in ("urllib3", "requests", "streamlit"):
        logging.getLogger(noisy).setLevel(max(level, logging.WARNING))

    _configured = True
