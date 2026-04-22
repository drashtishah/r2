"""
Purpose: render a static HTML report from the computed mean.
Input (stdin): none.
Output (stdout): HTML document with Method, Assumptions, and Result sections.
Stderr: info log with the mean read from disk.
Side effect: reads OUTPUTS_DIR / "mean.txt".
Example: python scripts/99_report.py > outputs/report.html
"""
import logging
import os
import sys

from constants import OUTPUTS_DIR

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    stream=sys.stderr,
    format="%(levelname)s script=%(name)s %(message)s",
)
log = logging.getLogger("99_report")

mean_str = (OUTPUTS_DIR / "mean.txt").read_text().strip()
log.info("mean=%s", mean_str)

html = f"""<!doctype html>
<html lang="en">
<head><meta charset="utf-8"><title>Toy pipeline report</title></head>
<body>
<h1>Toy pipeline report</h1>
<h2>Method</h2>
<p>Sample uniform floats in [0, 1), then compute the arithmetic mean.</p>
<h2>Assumptions</h2>
<ul>
  <li>The sample is drawn from a fixed seed for reproducibility.</li>
  <li>All values are finite and lie in [0, 1).</li>
</ul>
<h2>Result</h2>
<p>Mean: {mean_str}</p>
</body>
</html>
"""
sys.stdout.write(html)
