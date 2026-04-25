"""Map a markdown line number back to the PDF page it came from.

Markitdown leaves the original PDF page footers (`MSCI.COM | PAGE N OF M`)
embedded as plain text in the converted markdown. The footer for page N
appears at the END of that page's content, so a line L belongs to the
page named in the first footer at or after L.
"""
from __future__ import annotations

import re

_PAGE_FOOTER = re.compile(r"PAGE\s+(\d+)\s+OF\s+\d+", re.IGNORECASE)


def page_for_line(markdown_text: str, line: int) -> int | None:
    """Return the page number that owns `line` (1-indexed).

    Page N's footer is the last text on page N, so the page that owns a
    line is the one named in the first footer at or after the line.
    Returns None when no footer follows the line (rare; only when the
    citation is past the last footer in the document).
    """
    for i, text in enumerate(markdown_text.splitlines(), start=1):
        if i < line:
            continue
        match = _PAGE_FOOTER.search(text)
        if match:
            return int(match.group(1))
    return None
