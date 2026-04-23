"""Custom tools for the methbook pipeline.

list_existing_rules: pre-parsed catalog of every rule under
methbooks/rules/. Token-efficient catalog for planner and critique
agents (cheaper than re-reading every docstring).
"""
from __future__ import annotations

import ast
import json
import re
from pathlib import Path
from typing import Any

from claude_agent_sdk import tool

RULES_DIR = Path("methbooks/rules")

_FIELD_RE = re.compile(r"^(Purpose|Datapoints|Thresholds):\s*(.+)$", re.MULTILINE)


def _parse_rule(path: Path) -> dict[str, Any]:
    tree = ast.parse(path.read_text())
    funcs = [n for n in tree.body if isinstance(n, ast.FunctionDef)]
    if not funcs:
        return {"path": str(path), "name": path.stem, "purpose": "", "datapoints": [], "thresholds": ""}
    func = funcs[0]
    doc = ast.get_docstring(func) or ""
    fields: dict[str, str] = {}
    for m in _FIELD_RE.finditer(doc):
        fields[m.group(1).lower()] = m.group(2).strip().rstrip(".")
    datapoints = [d.strip() for d in fields.get("datapoints", "").split(",") if d.strip()]
    return {
        "path": str(path),
        "name": func.name,
        "purpose": fields.get("purpose", ""),
        "datapoints": datapoints,
        "thresholds": fields.get("thresholds", ""),
    }


def collect_existing_rules() -> list[dict[str, Any]]:
    if not RULES_DIR.exists():
        return []
    out: list[dict[str, Any]] = []
    for p in sorted(RULES_DIR.rglob("*.py")):
        if p.name == "__init__.py":
            continue
        out.append(_parse_rule(p))
    return out


@tool(
    "list_existing_rules",
    "List existing methbook rules with name, purpose, datapoints, and thresholds.",
    {},
)
async def list_existing_rules(args: dict[str, Any]) -> dict[str, Any]:
    rules = collect_existing_rules()
    return {"content": [{"type": "text", "text": json.dumps(rules, indent=2)}]}
