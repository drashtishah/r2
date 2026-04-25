"""Convert one PDF to Markdown.

Markdown is the derived artifact; the source PDF is the source of truth
and is kept on disk so reviewers can verify agent claims against the
original document. Pass --delete-pdf to remove the PDF after conversion.
"""
from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

from markitdown import MarkItDown

DEFAULT_OUT_DIR = Path("methbooks/data/markdown")


def convert(pdf_path: Path, out_dir: Path, keep_pdf: bool = True) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    md = MarkItDown().convert(str(pdf_path))
    out = out_dir / f"{pdf_path.stem}.md"
    out.write_text(md.text_content)
    if not keep_pdf:
        pdf_path.unlink()
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Convert a PDF to Markdown; the PDF is kept by default.")
    parser.add_argument("pdf", type=Path)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--delete-pdf", action="store_true", help="delete the source PDF after a successful conversion")
    args = parser.parse_args()

    logging.basicConfig(
        format="%(levelname)s script=%(name)s %(message)s",
        level=os.environ.get("LOG_LEVEL", "INFO"),
    )
    logger = logging.getLogger("pdf_to_md")

    keep_pdf = not args.delete_pdf
    out = convert(args.pdf, args.out_dir, keep_pdf=keep_pdf)
    logger.info("wrote=%s pdf_kept=%s", out, keep_pdf)


if __name__ == "__main__":
    main()
