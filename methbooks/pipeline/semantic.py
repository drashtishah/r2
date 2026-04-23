"""Run the semantic verifier against an existing run_dir.

Writes semantic_report.json into the run_dir. Exits non-zero if overall
is not "pass".

Usage:
    python -m methbooks.pipeline.semantic --run-dir <meth-pipeline/slug/ts>
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", required=True, type=Path)
    args = parser.parse_args()
    from methbooks.pipeline.agents import run_semantic_verifier

    run_dir: Path = args.run_dir
    report = asyncio.run(
        run_semantic_verifier(run_dir, run_dir.parent.name, run_dir.name)
    )
    (run_dir / "semantic_report.json").write_text(json.dumps(report, indent=2))

    if report.get("overall") != "pass":
        sys.exit(1)


if __name__ == "__main__":
    main()
