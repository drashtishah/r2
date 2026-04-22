"""
Purpose: compute the arithmetic mean of a JSON array of floats.
Input (stdin): JSON array of floats.
Output (stdout): none.
Stderr: info log with the computed mean.
Side effect: writes the mean to OUTPUTS_DIR / "mean.txt" with MEAN_PRECISION.
Example: python scripts/02_mean.py < data/01_sample.json
"""
import json
import logging
import os
import statistics
import sys

from constants import MEAN_PRECISION, OUTPUTS_DIR

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    stream=sys.stderr,
    format="%(levelname)s script=%(name)s %(message)s",
)
log = logging.getLogger("02_mean")


def compute_mean(values: list[float]) -> float:
    assert len(values) > 0, "empty input"
    return statistics.fmean(values)


raw = json.load(sys.stdin)
mean = compute_mean(raw)
OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
(OUTPUTS_DIR / "mean.txt").write_text(f"{mean:.{MEAN_PRECISION}f}\n")
log.info("mean=%.*f", MEAN_PRECISION, mean)
