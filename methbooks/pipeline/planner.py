"""Run the planner stage against an existing run_dir.

Usage:
    python -m methbooks.pipeline.planner --run-dir <meth-pipeline/slug/ts>
"""
from __future__ import annotations

import argparse
import asyncio
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", required=True, type=Path)
    args = parser.parse_args()
    from methbooks.pipeline.agents import run_planner

    run_dir: Path = args.run_dir
    asyncio.run(run_planner(run_dir, run_dir.parent.name, run_dir.name))


if __name__ == "__main__":
    main()
