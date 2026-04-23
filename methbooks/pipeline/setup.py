"""Bootstrap a methbook run: create run_dir, seed markdown, create branch.

Accepts a PDF (runs pdf_to_md) or a pre-extracted .md (copies it into
methbooks/data/markdown/ if not already there). Prints the run_dir to
stdout; everything else goes to stderr so callers can capture just the
path with command substitution.

Usage:
    python -m methbooks.pipeline.setup <path.pdf-or-md>
"""
from __future__ import annotations

import argparse
import datetime
import subprocess
import sys
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("src", type=Path, help="PDF or markdown path.")
    args = parser.parse_args()
    _run(args.src)


def _run(src: Path) -> None:
    if not src.exists():
        sys.exit(f"source not found: {src}")

    slug = src.stem
    ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    run_dir = Path("meth-pipeline") / slug / ts
    (run_dir / "input").mkdir(parents=True, exist_ok=True)

    md_target = Path("methbooks/data/markdown") / f"{slug}.md"
    md_target.parent.mkdir(parents=True, exist_ok=True)

    if src.suffix == ".md":
        if src.resolve() != md_target.resolve():
            md_target.write_text(src.read_text())
    elif src.suffix == ".pdf":
        rc = subprocess.call(
            ["uv", "run", "python", "-m", "methbooks.pdf_to_md",
             str(src), "--out-dir", str(md_target.parent)],
            stdout=sys.stderr, stderr=sys.stderr,
        )
        if rc != 0 or not md_target.exists():
            sys.exit(f"pdf_to_md failed: rc={rc}")
    else:
        sys.exit(f"unsupported suffix: {src.suffix!r}")

    (run_dir / "input" / "markdown.md").write_text(md_target.read_text())

    branch = f"methbook/{slug}"
    subprocess.check_call(
        ["git", "checkout", "-b", branch],
        stdout=sys.stderr, stderr=sys.stderr,
    )

    print(str(run_dir))


if __name__ == "__main__":
    main()
