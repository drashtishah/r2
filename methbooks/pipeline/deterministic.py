"""Run the deterministic verifier against an existing run_dir.

Writes deterministic_report.json into the run_dir. Exits non-zero if any
check fails.

Usage:
    python -m methbooks.pipeline.deterministic --run-dir <meth-pipeline/slug/ts>
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", required=True, type=Path)
    args = parser.parse_args()
    from methbooks.pipeline.logging import log_event
    from methbooks.pipeline.schemas.plan import Methbook
    from methbooks.pipeline.verifier_checks import run_deterministic_checks

    run_dir: Path = args.run_dir
    slug = run_dir.parent.name
    plan = Methbook.model_validate_json((run_dir / "methbook_v2.json").read_text())

    log_event("deterministic_verifier", "agent_start")
    report = run_deterministic_checks(plan, f"main...methbook/{slug}")
    (run_dir / "deterministic_report.json").write_text(json.dumps(report, indent=2))
    log_event("deterministic_verifier", "agent_end", overall=report["overall"])

    if report["overall"] != "pass":
        sys.exit(1)


if __name__ == "__main__":
    main()
