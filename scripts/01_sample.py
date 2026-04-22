"""
Purpose: emit a deterministic JSON array of random floats in [0, 1).
Input (stdin): none.
Output (stdout): JSON array of SAMPLE_SIZE floats.
Stderr: info log with the count; debug log per value.
Example: python scripts/01_sample.py > data/01_sample.json
"""
import json
import logging
import os
import random
import sys

from constants import RANDOM_SEED, SAMPLE_SIZE

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    stream=sys.stderr,
    format="%(levelname)s script=%(name)s %(message)s",
)
log = logging.getLogger("01_sample")

random.seed(RANDOM_SEED)
values = [random.random() for _ in range(SAMPLE_SIZE)]
for v in values:
    log.debug("value=%s", v)
json.dump(values, sys.stdout)
log.info("count=%d", len(values))
