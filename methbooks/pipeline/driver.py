"""Driver: orchestrates a single methbook run end-to-end.

Usage:
    python -m methbooks.pipeline.driver <pdf_path>
"""
from __future__ import annotations

import argparse
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the methbook pipeline on one PDF."
    )
    parser.add_argument("pdf", type=Path, help="Path to the source PDF.")
    args = parser.parse_args()
    _run(args.pdf)


def _run(pdf: Path) -> None:
    # Heavy imports inside _run keep `--help` cheap and avoid pulling
    # the SDK at import time.
    import asyncio
    import datetime
    import json
    import subprocess
    import sys

    from methbooks.pipeline.agents import (
        run_critique,
        run_implementer,
        run_planner,
        run_semantic_verifier,
    )
    from methbooks.pipeline.logging import log_event
    from methbooks.pipeline.verifier_checks import run_deterministic_checks

    slug = pdf.stem
    ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    run_id = f"{slug}/{ts}"
    run_dir = Path("meth-pipeline") / slug / ts

    if not pdf.exists():
        (run_dir / "input").mkdir(parents=True, exist_ok=True)
        log_event("driver", "error", note=f"pdf not found: {pdf}", run_id=run_id)
        sys.exit(1)

    (run_dir / "input").mkdir(parents=True, exist_ok=True)
    log_event("driver", "pipeline_start", run_id=run_id, pdf=str(pdf))

    # 1. pdf_to_md (subprocess: driver stays a thin orchestrator)
    log_event("driver", "stage_start", stage="pdf_to_md")
    md_target_dir = Path("methbooks/data/markdown")
    md_target_dir.mkdir(parents=True, exist_ok=True)
    rc = subprocess.call([
        "uv", "run", "python", "-m", "methbooks.pdf_to_md",
        str(pdf), "--out-dir", str(md_target_dir),
    ])
    if rc != 0:
        log_event("driver", "error", stage="pdf_to_md", exit_code=rc)
        sys.exit(1)
    md_file = md_target_dir / f"{slug}.md"
    if not md_file.exists():
        log_event("driver", "error", stage="pdf_to_md", note=f"no markdown at {md_file}")
        sys.exit(1)
    (run_dir / "input" / "markdown.md").write_text(md_file.read_text())
    log_event("driver", "stage_end", stage="pdf_to_md", out=str(md_file))

    # 2. Feature branch
    branch = f"methbook/{slug}"
    log_event("driver", "stage_start", stage="branch", branch=branch)
    subprocess.check_call(["git", "checkout", "-b", branch])
    log_event("driver", "stage_end", stage="branch")

    # 3. Planner
    asyncio.run(run_planner(run_dir, slug, ts))

    # 4. Critique (returns the authoritative plan_v2)
    plan_v2 = asyncio.run(run_critique(run_dir, slug, ts))

    # 5. Implementer
    asyncio.run(run_implementer(run_dir, slug, ts))

    # 6. Deterministic verifier
    log_event("deterministic_verifier", "agent_start")
    det_report = run_deterministic_checks(plan_v2, f"main...{branch}")
    (run_dir / "deterministic_report.json").write_text(json.dumps(det_report, indent=2))
    log_event("deterministic_verifier", "agent_end", overall=det_report["overall"])
    if det_report["overall"] != "pass":
        log_event("driver", "error", stage="deterministic", overall="fail")
        sys.exit(1)

    # 7. Semantic verifier
    sem_report = asyncio.run(run_semantic_verifier(run_dir, slug, ts))
    (run_dir / "semantic_report.json").write_text(json.dumps(sem_report, indent=2))
    if sem_report.get("overall") != "pass":
        log_event("driver", "error", stage="semantic", overall=str(sem_report.get("overall")))
        sys.exit(1)

    # 8. Push and open auto-merge PR
    log_event("driver", "stage_start", stage="pr")
    subprocess.check_call(["git", "push", "-u", "origin", branch])
    body = json.dumps(plan_v2.model_dump(), indent=2)
    subprocess.check_call([
        "gh", "pr", "create", "--title", f"methbook: {slug}", "--body", body,
    ])
    subprocess.check_call([
        "gh", "pr", "merge", "--auto", "--merge", "--delete-branch",
    ])
    log_event("driver", "stage_end", stage="pr")

    log_event("driver", "pipeline_end", run_id=run_id)


if __name__ == "__main__":
    main()
