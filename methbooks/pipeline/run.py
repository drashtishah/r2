"""End-to-end methbook pipeline driver.

Chains setup -> planner -> critique -> implementer -> deterministic ->
semantic -> rules_index -> pr as subprocess calls, replacing the Makefile
`methbook` target's shell-glued chain. Using subprocess list-form
eliminates the two-layer (make + shell) quoting that produced the GICS
breakages (see PRs #14 and #21).

Usage:
    python -m methbooks.pipeline.run <path.pdf-or-md>

Per-stage `make` targets (planner, critique, ...) still work for
debugging or resuming a partially-completed run; this driver is the
blessed path for a fresh end-to-end run.
"""
from __future__ import annotations

import argparse
import subprocess
from pathlib import Path

_STAGES_WITH_RUN_DIR = (
    "planner",
    "critique",
    "implementer",
    "deterministic",
    "semantic",
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("src", type=Path, help="PDF or markdown path.")
    args = parser.parse_args()

    result = subprocess.run(
        ["uv", "run", "python", "-m", "methbooks.pipeline.setup", str(args.src)],
        stdout=subprocess.PIPE,
        text=True,
        check=True,
    )
    run_dir = Path(result.stdout.strip())

    for stage in _STAGES_WITH_RUN_DIR:
        subprocess.check_call([
            "uv", "run", "python", "-m", f"methbooks.pipeline.{stage}",
            "--run-dir", str(run_dir),
        ])

    subprocess.check_call(["uv", "run", "python", "-m", "methbooks.pipeline.rules_index"])

    subprocess.check_call([
        "uv", "run", "python", "-m", "methbooks.pipeline.pr",
        "--run-dir", str(run_dir),
    ])


if __name__ == "__main__":
    main()
