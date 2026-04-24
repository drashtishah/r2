"""End-to-end methbook pipeline driver.

Chains setup -> planner -> critique -> commit_plan -> implementer ->
deterministic -> semantic -> rules_index -> pr as subprocess calls,
replacing the Makefile `methbook` target's shell-glued chain. Using
subprocess list-form eliminates the two-layer (make + shell) quoting
that produced the GICS breakages (see PRs #14 and #21).

commit_plan runs right after critique because the plan is stable
there; implementer and later stages consume it but do not mutate it,
so committing the sidecar early is safe.

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


def _stage(module: str, run_dir: Path | None = None) -> None:
    cmd = ["uv", "run", "python", "-m", f"methbooks.pipeline.{module}"]
    if run_dir is not None:
        cmd += ["--run-dir", str(run_dir)]
    subprocess.check_call(cmd)


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

    _stage("planner", run_dir)
    _stage("critique", run_dir)
    _stage("commit_plan", run_dir)
    _stage("implementer", run_dir)
    _stage("deterministic", run_dir)
    _stage("semantic", run_dir)
    _stage("rules_index")
    _stage("pr", run_dir)


if __name__ == "__main__":
    main()
