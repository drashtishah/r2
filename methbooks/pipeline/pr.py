"""Push the methbook branch and open an auto-merge PR.

Runs at the end of the methbook pipeline chain. Uses subprocess list-form
calls so neither the slug nor the display name has to survive shell
quoting; we can therefore keep `methbook_v2.json` out of the PR body
and use the pretty display name (parens, ® and all) in the PR title
without risking the 65 KiB body limit or shell syntax errors.

Usage:
    python -m methbooks.pipeline.pr --run-dir <meth-pipeline/slug/ts>
"""
from __future__ import annotations

import argparse
import subprocess
from pathlib import Path

BODY = (
    "Methbook pipeline output for `{display}`. "
    "See branch commits for the generated module, data dictionary, and rule files."
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", required=True, type=Path)
    args = parser.parse_args()
    run_dir: Path = args.run_dir

    slug = run_dir.parent.name
    display_path = run_dir / "input" / "display_name.txt"
    display = display_path.read_text().strip() if display_path.exists() else slug

    branch = f"methbook/{slug}"
    title = f"methbook: {display}"
    body = BODY.format(display=display)

    subprocess.check_call(["git", "push", "-u", "origin", branch])
    subprocess.check_call(["gh", "pr", "create", "--title", title, "--body", body])
    subprocess.check_call(["gh", "pr", "merge", "--auto", "--merge", "--delete-branch"])


if __name__ == "__main__":
    main()
