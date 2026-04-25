"""Download an MSCI methodology PDF by code.

MSCI exposes each methodology at a stable URL keyed by short code:
    https://www.msci.com/index/methodology/latest/<CODE>
The response is the PDF directly (Content-Type: application/octet-stream),
with the canonical filename in Content-Disposition. The same URLs are
embedded in the static page HTML at /indexes/index-resources/index-methodology
and are what the on-page "Search by name or code" widget resolves to.

Saves to methbooks/data/pdfs/<filename> using the server's filename. If a
file with that basename already exists, exits 0 and prints nothing
(idempotent re-run).
"""
import argparse
import re
import sys
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[2]
DOWNLOAD_DIR = ROOT / "methbooks" / "data" / "pdfs"
URL_TEMPLATE = "https://www.msci.com/index/methodology/latest/{code}"
HEADERS = {"User-Agent": "methbooks-fetcher/1.0"}

_FILENAME_RE = re.compile(r'filename="([^"]+)"')


def fetch(code: str) -> Path | None:
    """Download the methodology for ``code``. Returns the saved Path, or
    None if the destination filename already existed (no overwrite)."""
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    url = URL_TEMPLATE.format(code=code)
    req = Request(url, headers=HEADERS)
    with urlopen(req, timeout=120) as r:
        cd = r.headers.get("Content-Disposition", "")
        m = _FILENAME_RE.search(cd)
        if not m:
            print(f"no filename in Content-Disposition: {cd!r}", file=sys.stderr)
            sys.exit(1)
        target = DOWNLOAD_DIR / Path(m.group(1)).name
        if target.exists():
            return None
        tmp = target.with_suffix(target.suffix + ".part")
        try:
            with open(tmp, "wb") as f:
                while chunk := r.read(64 * 1024):
                    f.write(chunk)
            tmp.replace(target)
        except BaseException:
            tmp.unlink(missing_ok=True)
            raise
    return target


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--code", required=True,
                    help="MSCI methodology code, e.g. GIMI, IndexCalc, CE")
    args = ap.parse_args()
    try:
        target = fetch(args.code)
    except HTTPError as e:
        print(f"HTTP {e.code} {e.reason} for code={args.code}", file=sys.stderr)
        return 1
    if target is None:
        return 0
    print(target.relative_to(ROOT))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
