#!/usr/bin/env python3
"""Assert every path referenced in markdown and JSON resolves on disk.

Walks .md and .json files, extracts backtick-wrapped (markdown) and
quoted (JSON) strings that look like project paths, and checks each
one exists under the repo root. Catches skill drift when a referenced
file is renamed, moved, or deleted.

Run:
    python3 tests/test_paths.py
    python3 -m unittest discover tests
    pytest tests/
"""

import difflib
import os
import re
import unittest
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Set

ROOT = Path(__file__).resolve().parent.parent

SCAN_EXTENSIONS: Set[str] = {'.md', '.json'}
EXCLUDE_DIRS: Set[str] = {'.git', '.venv'}

PATH_CHAR_RE = re.compile(r'^[a-z0-9./\-_{}\$]+$')


@dataclass(frozen=True)
class PathEntry:
    file: str
    path: str
    line_number: int


def collect_files(root: Path) -> List[Path]:
    files: List[Path] = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in EXCLUDE_DIRS]
        for fname in filenames:
            fpath = Path(dirpath) / fname
            if fpath.suffix in SCAN_EXTENSIONS:
                files.append(fpath)
    return sorted(files)


def normalize_path(raw: str) -> Optional[str]:
    cleaned = raw.strip().rstrip('.,;:)`')
    if '/' not in cleaned:
        return None
    if not PATH_CHAR_RE.match(cleaned):
        return None
    if re.match(r'\d+\.\d+\.\d+', cleaned):
        return None
    if cleaned.startswith('./'):
        cleaned = cleaned[2:]
    if cleaned.startswith('/'):
        return None
    if not cleaned.replace('/', ''):
        return None
    return cleaned or None


def extract_from_md(content: str, rel_path: str) -> List[PathEntry]:
    entries: List[PathEntry] = []
    for line_num, line in enumerate(content.split('\n'), start=1):
        for match in re.finditer(r'`([^`\n]*?/[^`\n]*?)`', line):
            normalized = normalize_path(match.group(1))
            if normalized:
                entries.append(PathEntry(rel_path, normalized, line_num))
    return entries


def extract_from_json(content: str, rel_path: str) -> List[PathEntry]:
    entries: List[PathEntry] = []
    for line_num, line in enumerate(content.split('\n'), start=1):
        for match in re.finditer(r'"([^"]*?/[^"]*?)"', line):
            normalized = normalize_path(match.group(1))
            if normalized:
                entries.append(PathEntry(rel_path, normalized, line_num))
    return entries


def is_template(path: str) -> bool:
    return any(c in path for c in '{}$')


def extract_all() -> List[PathEntry]:
    extractors = {'.md': extract_from_md, '.json': extract_from_json}
    entries: List[PathEntry] = []
    for fpath in collect_files(ROOT):
        rel = str(fpath.relative_to(ROOT))
        extractor = extractors.get(fpath.suffix)
        if not extractor:
            continue
        content = fpath.read_text(encoding='utf-8')
        entries.extend(extractor(content, rel))
    return entries


def all_repo_paths() -> List[str]:
    paths: List[str] = []
    for dirpath, dirnames, filenames in os.walk(ROOT):
        dirnames[:] = [d for d in dirnames if d not in EXCLUDE_DIRS]
        rel_dir = Path(dirpath).relative_to(ROOT)
        for name in list(dirnames) + filenames:
            p = str(rel_dir / name) if str(rel_dir) != '.' else name
            paths.append(p)
    return paths


def find_missing(entries: List[PathEntry]) -> List[PathEntry]:
    missing: List[PathEntry] = []
    seen: Set[PathEntry] = set()
    for e in entries:
        if e in seen:
            continue
        seen.add(e)
        if is_template(e.path):
            continue
        if not (ROOT / e.path).exists():
            missing.append(e)
    return sorted(missing, key=lambda e: (e.file, e.line_number, e.path))


def format_missing(missing: List[PathEntry]) -> str:
    candidates = all_repo_paths()
    lines = [f"{len(missing)} referenced path(s) not found:"]
    for e in missing:
        lines.append(f"{e.file}:{e.line_number}: {e.path} (not found)")
        matches = difflib.get_close_matches(e.path, candidates, n=3, cutoff=0.7)
        if matches:
            lines.append(f"  suggestions: {', '.join(matches)}")
    return '\n'.join(lines)


class PathIntegrity(unittest.TestCase):
    def test_all_referenced_paths_exist(self) -> None:
        missing = find_missing(extract_all())
        if missing:
            self.fail('\n' + format_missing(missing))


if __name__ == "__main__":
    unittest.main()
