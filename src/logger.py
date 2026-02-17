"""Logging setup — file output + rich console output."""

from __future__ import annotations

import logging
from pathlib import Path

from rich.console import Console
from rich.logging import RichHandler

_configured = False


def setup_logging(level: str = "INFO", log_file: str = "migration.log") -> logging.Logger:
    """Configure and return the root application logger.

    * Logs to a file at *log_file* with full timestamps and DEBUG detail.
    * Logs to the console via Rich with the caller-specified *level*.
    * Safe to call multiple times — only configures handlers once.
    """
    global _configured
    logger = logging.getLogger("migration")

    if _configured:
        return logger

    logger.setLevel(logging.DEBUG)  # capture everything; handlers filter

    # --- File handler (always DEBUG) ---
    log_path = Path(log_file)
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(
        "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    logger.addHandler(fh)

    # --- Rich console handler ---
    console = Console(stderr=True)
    rh = RichHandler(
        console=console,
        show_time=True,
        show_path=False,
        rich_tracebacks=True,
        tracebacks_show_locals=False,
        markup=True,
    )
    rh.setLevel(getattr(logging, level.upper(), logging.INFO))
    logger.addHandler(rh)

    _configured = True
    return logger


def get_logger(name: str | None = None) -> logging.Logger:
    """Return a child logger under the ``migration`` namespace."""
    base = "migration"
    if name:
        return logging.getLogger(f"{base}.{name}")
    return logging.getLogger(base)
