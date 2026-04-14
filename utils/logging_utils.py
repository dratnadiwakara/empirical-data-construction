"""
Structured logging utilities for the empirical-data-construction pipeline.
"""
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


class _JsonFormatter(logging.Formatter):
    """Emit log records as newline-delimited JSON."""

    def format(self, record: logging.LogRecord) -> str:
        entry: dict = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if getattr(record, "data", None):
            entry["data"] = record.data
        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry, default=str)


_TEXT_FMT = logging.Formatter(
    "%(asctime)s [%(levelname)-8s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def get_logger(
    name: str,
    level: int = logging.INFO,
    log_file: Optional[Path] = None,
) -> logging.Logger:
    """
    Return a logger with a human-readable console handler and an optional
    JSON-format file handler.

    Parameters
    ----------
    name : str
        Logger name (pass __name__ from the calling module).
    level : int
        Logging level (default INFO).
    log_file : Path, optional
        If provided, also write JSON-formatted logs to this file.
    """
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(level)

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(_TEXT_FMT)
    logger.addHandler(console)

    if log_file is not None:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(_JsonFormatter())
        logger.addHandler(fh)

    logger.propagate = False
    return logger


def log_step(logger: logging.Logger, step: str, **kwargs) -> None:
    """
    Emit a structured INFO entry for a pipeline step with keyword metadata.

    Example
    -------
    log_step(logger, "download_complete", year=2024, bytes=1_234_567, url="...")
    """
    record = logger.makeRecord(
        logger.name, logging.INFO, fn="", lno=0, msg=step, args=(), exc_info=None
    )
    record.data = kwargs  # type: ignore[attr-defined]
    logger.handle(record)
