"""Regenerate methbooks/rules_index.json from methodology imports.

Walks methbooks/methodologies/**/*.py, collects `from methbooks.rules.<cat>.<rule>
import ...` statements via the ast module, and writes an index mapping every
methodology slug to the rules it imports, plus an inverse rule-to-methodologies
map and a list of orphan rule files not imported anywhere. Commits the update
when the file changes (same pattern as verifier auto-repairs).

Usage:
    python -m methbooks.pipeline.rules_index
"""
from __future__ import annotations

import ast
import json
import subprocess
from pathlib import Path
from typing import TypedDict

METHODOLOGIES = Path("methbooks/methodologies")
RULES_DIR = Path("methbooks/rules")
RULES_PKG = "methbooks.rules"
OUTPUT = Path("methbooks/rules_index.json")


def _rule_imports(path: Path) -> list[str]:
    tree = ast.parse(path.read_text())
    found: set[str] = set()
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.ImportFrom)
            and node.module
            and node.module.startswith(RULES_PKG + ".")
        ):
            found.add(node.module[len(RULES_PKG) + 1 :])
    return sorted(found)


def _slug(path: Path) -> str:
    return str(path.relative_to(METHODOLOGIES).with_suffix(""))


def _present_rules() -> set[str]:
    found: set[str] = set()
    for p in RULES_DIR.rglob("*.py"):
        if p.name == "__init__.py":
            continue
        rel = p.relative_to(RULES_DIR)
        found.add(f"{rel.parent}.{rel.stem}")
    return found


class Index(TypedDict):
    methodologies: dict[str, list[str]]
    rules: dict[str, list[str]]
    orphans: list[str]


def build_index() -> Index:
    by_meth: dict[str, list[str]] = {}
    for p in sorted(METHODOLOGIES.rglob("*.py")):
        if p.name == "__init__.py":
            continue
        by_meth[_slug(p)] = _rule_imports(p)

    by_rule: dict[str, list[str]] = {}
    for meth, rules in by_meth.items():
        for r in rules:
            by_rule.setdefault(r, []).append(meth)
    by_rule = {k: sorted(v) for k, v in sorted(by_rule.items())}

    orphans = sorted(_present_rules() - by_rule.keys())

    return {"methodologies": by_meth, "rules": by_rule, "orphans": orphans}


def _commit_if_changed() -> None:
    subprocess.check_call(["git", "add", str(OUTPUT)])
    if subprocess.call(
        ["git", "diff", "--cached", "--quiet", str(OUTPUT)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    ) != 0:
        subprocess.check_call(
            ["git", "commit", "-m", "index: refresh rules_index"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )


def main() -> None:
    from methbooks.pipeline.logging import log_event

    log_event("rules_index", "agent_start")
    index = build_index()
    OUTPUT.write_text(json.dumps(index, indent=2) + "\n")
    _commit_if_changed()
    log_event(
        "rules_index",
        "agent_end",
        methodologies=len(index["methodologies"]),
        rules=len(index["rules"]),
        orphans=len(index["orphans"]),
    )


if __name__ == "__main__":
    main()
