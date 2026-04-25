"""Download MSCI methodology PDFs by code.

Two flows, dispatched on code shape:

* Methodology code (alphabetic, e.g. GIMI, IndexCalc, CE):
  https://www.msci.com/index/methodology/latest/<CODE> returns one PDF
  with the canonical filename in Content-Disposition.

* Index code (all digits, e.g. 892400 ACWI, 760074):
  https://www-cdn.msci.com/BMREGDownload/getZipFile?indexCode=<CODE>&date=latest
  returns a zip containing all PDFs governing that index (GIMI, IndexCalc,
  Corporate Events, the index-specific methodology, etc.). Members are
  prefixed `0_`, `1_`, `2_` by document priority; we strip the prefix
  when saving so basenames match other flows and dedupe cleanly.

Saves to methbooks/data/pdfs/. If a basename is already present there,
the corresponding PDF is skipped (existing file wins; no overwrite).
"""
import argparse
import io
import re
import sys
import time
import zipfile
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[2]
DOWNLOAD_DIR = ROOT / "methbooks" / "data" / "pdfs"
HEADERS = {"User-Agent": "methbooks-fetcher/1.0"}

METH_URL = "https://www.msci.com/index/methodology/latest/{code}"
ZIP_URL = (
    "https://www-cdn.msci.com/BMREGDownload/getZipFile"
    "?formDate={ts}&indexCode={code}&text-search=&date=latest&custom-date="
)
ZIP_REFERER = (
    "https://www-cdn.msci.com/widget/web/cdn/index-methodology/-/"
    "msciregulationportlet_WAR_msciregulationportlet_INSTANCE_80T4AomZQT8F"
)

_FILENAME_RE = re.compile(r'filename="([^"]+)"')
_ZIP_PREFIX_RE = re.compile(r"^\d+_")


def _save(content: bytes, name: str) -> Path | None:
    """Atomically save ``content`` as ``DOWNLOAD_DIR/name``. Returns the
    path on first write, None if the file already existed."""
    target = DOWNLOAD_DIR / name
    if target.exists():
        return None
    tmp = target.with_suffix(target.suffix + ".part")
    try:
        tmp.write_bytes(content)
        tmp.replace(target)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
    return target


def fetch_methodology(code: str) -> list[Path]:
    """Download a single methodology PDF by alphabetic code."""
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    req = Request(METH_URL.format(code=code), headers=HEADERS)
    with urlopen(req, timeout=120) as r:
        cd = r.headers.get("Content-Disposition", "")
        m = _FILENAME_RE.search(cd)
        if not m:
            print(f"no filename in Content-Disposition: {cd!r}", file=sys.stderr)
            sys.exit(1)
        body = r.read()
    saved = _save(body, Path(m.group(1)).name)
    return [saved] if saved else []


def fetch_index_zip(code: str) -> list[Path]:
    """Download the bundled methodology zip for a numeric index code,
    extract every PDF that is not already on disk, and return the list of
    new paths."""
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    url = ZIP_URL.format(ts=int(time.time() * 1000), code=code)
    req = Request(url, headers={**HEADERS, "Referer": ZIP_REFERER})
    with urlopen(req, timeout=300) as r:
        body = r.read()
    new_paths: list[Path] = []
    with zipfile.ZipFile(io.BytesIO(body)) as zf:
        for member in zf.namelist():
            if not member.lower().endswith(".pdf"):
                continue
            name = _ZIP_PREFIX_RE.sub("", Path(member).name)
            saved = _save(zf.read(member), name)
            if saved:
                new_paths.append(saved)
    return new_paths


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--code", required=True,
                    help="MSCI methodology code (e.g. GIMI, IndexCalc, CE) "
                         "or numeric index code (e.g. 892400)")
    args = ap.parse_args()
    try:
        if args.code.isdigit():
            new_paths = fetch_index_zip(args.code)
        else:
            new_paths = fetch_methodology(args.code)
    except HTTPError as e:
        print(f"HTTP {e.code} {e.reason} for code={args.code}", file=sys.stderr)
        return 1
    for p in new_paths:
        print(p.relative_to(ROOT))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
