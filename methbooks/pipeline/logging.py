"""Central pipeline logging.

Two handlers attached to one logger: stderr (human-readable) and
logs/agents.jsonl (one JSON object per line). All pipeline code emits via
log_event(role, event, **fields); the SDK pre_tool_use / post_tool_use
hooks wired in agents.py also funnel through here.
"""
from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

LOG_DIR = Path("logs")
LOG_FILE = LOG_DIR / "agents.jsonl"


class _JsonlHandler(logging.Handler):
    def __init__(self, path: Path) -> None:
        super().__init__()
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def emit(self, record: logging.LogRecord) -> None:
        payload = getattr(record, "payload", None)
        if payload is None:
            return
        with self.path.open("a") as f:
            f.write(json.dumps(payload, separators=(",", ":")))
            f.write("\n")


_logger: logging.Logger | None = None


def _get_logger() -> logging.Logger:
    global _logger
    if _logger is not None:
        return _logger
    logger = logging.getLogger("methbooks.pipeline")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    stderr = logging.StreamHandler(sys.stderr)
    stderr.setFormatter(
        logging.Formatter("%(levelname)s script=%(name)s role=%(role)s %(message)s")
    )
    logger.addHandler(stderr)
    logger.addHandler(_JsonlHandler(LOG_FILE))

    _logger = logger
    return logger


def log_event(role: str, event: str, **fields: Any) -> None:
    payload: dict[str, Any] = {
        "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "role": role,
        "event": event,
    }
    payload.update(fields)
    msg = "event=" + event
    if fields:
        msg += " " + " ".join(f"{k}={v}" for k, v in fields.items())
    _get_logger().info(msg, extra={"role": role, "payload": payload})
