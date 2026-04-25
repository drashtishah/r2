"""Commit the methbook_v2.json plan to a tracked sidecar path.

Critique produces run_dir/methbook_v2.json, which is the authoritative
plan (new_rules, reused_rules, composition_order, identification, ...).
The run_dir itself is gitignored, so once the methbook PR merges the
plan is lost from main. Cross-methbook reuse tracking is the casualty:
reused_rules are not necessarily imported by the methodology module
(event_handling rules are referenced but applied out-of-band), so the
rules_index built from imports alone undercounts reuse.

This script copies run_dir/methbook_v2.json to
methbooks/methodologies/{provider}/{slug}_plan.json and commits the
sidecar if it changed. methbooks.pipeline.rules_index reads these
sidecars to surface full reuse information.

Usage:
    python -m methbooks.pipeline.commit_plan --run-dir <meth-pipeline/slug/ts>
"""
from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path

from methbooks.pipeline.page_lookup import page_for_line


def _populate_pages(plan: dict, markdown_text: str) -> None:
    """Fill source.page on every new_rule from the markdown footers in place."""
    for rule in plan.get("new_rules", []):
        source = rule.get("source")
        if not source or source.get("page") is not None:
            continue
        line = source.get("line")
        if isinstance(line, int):
            source["page"] = page_for_line(markdown_text, line)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", required=True, type=Path)
    args = parser.parse_args()
    run_dir: Path = args.run_dir

    plan_src = run_dir / "methbook_v2.json"
    plan = json.loads(plan_src.read_text())
    ident = plan["identification"]
    provider = ident["provider"]
    slug = ident["slug"]

    markdown_text = (run_dir / "input" / "markdown.md").read_text()
    _populate_pages(plan, markdown_text)

    target = Path(f"methbooks/methodologies/{provider}/{slug}_plan.json")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(plan, indent=2) + "\n")

    subprocess.check_call(["git", "add", str(target)])
    if subprocess.call(
        ["git", "diff", "--cached", "--quiet", str(target)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    ) != 0:
        subprocess.check_call(
            ["git", "commit", "-m", f"plan: commit {provider}/{slug}_plan.json sidecar"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )


if __name__ == "__main__":
    main()
