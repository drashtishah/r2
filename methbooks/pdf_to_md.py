"""Convert one PDF to Markdown, then delete the PDF.

Markdown is the retained artifact. The source PDF is removed after a
successful conversion so the repo never accumulates binaries. Use
--keep-pdf to override.
"""
from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

from markitdown import MarkItDown

DEFAULT_OUT_DIR = Path("methbooks/data/markdown")


def convert(pdf_path: Path, out_dir: Path, keep_pdf: bool = False) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    md = MarkItDown().convert(str(pdf_path))
    out = out_dir / f"{pdf_path.stem}.md"
    out.write_text(md.text_content)
    if not keep_pdf:
        pdf_path.unlink()
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Convert a PDF to Markdown and delete the PDF.")
    parser.add_argument("pdf", type=Path)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--keep-pdf", action="store_true", help="keep the source PDF instead of deleting it")
    args = parser.parse_args()

    logging.basicConfig(
        format="%(levelname)s script=%(name)s %(message)s",
        level=os.environ.get("LOG_LEVEL", "INFO"),
    )
    logger = logging.getLogger("pdf_to_md")

    out = convert(args.pdf, args.out_dir, keep_pdf=args.keep_pdf)
    logger.info("wrote=%s pdf_deleted=%s", out, not args.keep_pdf)


if __name__ == "__main__":
    main()
