"""Fold methodology->rule import edges into graphify's graph.json.

Graphify's AST pass emits file-contains-function edges but does not
follow `from methbooks.rules.<cat>.<name> import ...` statements to
create edges between a methodology file and the rule files it imports.
This script walks the committed `methbooks/rules_index.json` (produced
by `methbooks.pipeline.rules_index`) and appends one `imports` edge per
methodology -> rule pair, matching file-level nodes by `source_file`.

Idempotent: repeat invocations skip edges already present, so running
`make graph` multiple times or running enrich_graph alone after a
graphify refresh is safe.

Usage:
    python -m methbooks.pipeline.enrich_graph
"""
from __future__ import annotations

import json
from pathlib import Path

GRAPH = Path("methbooks/graphify-out/graph.json")
INDEX = Path("methbooks/rules_index.json")


def _methodology_path(slug: str) -> str:
    return f"methbooks/methodologies/{slug}.py"


def _rule_path(rule: str) -> str:
    category, name = rule.split(".", 1)
    return f"methbooks/rules/{category}/{name}.py"


def _file_nodes(graph: dict) -> dict[str, str]:
    lookup: dict[str, str] = {}
    for node in graph["nodes"]:
        if node.get("source_location") == "L1":
            lookup[node["source_file"]] = node["id"]
    return lookup


def main() -> None:
    from methbooks.pipeline.logging import log_event

    log_event("enrich_graph", "agent_start")

    if not GRAPH.exists():
        log_event("enrich_graph", "skip", note="no graph.json; run `make graph` first")
        return

    graph = json.loads(GRAPH.read_text())
    index = json.loads(INDEX.read_text())

    links_key = "links" if "links" in graph else "edges"
    by_path = _file_nodes(graph)
    existing: set[tuple[str, str, str]] = {
        (edge["source"], edge["target"], edge.get("relation", ""))
        for edge in graph[links_key]
    }

    added = 0
    missing_methodology: list[str] = []
    missing_rule: list[str] = []
    for methodology, rules in index["methodologies"].items():
        methodology_path = _methodology_path(methodology)
        methodology_id = by_path.get(methodology_path)
        if methodology_id is None:
            missing_methodology.append(methodology_path)
            continue
        for rule in rules:
            rule_path = _rule_path(rule)
            rule_id = by_path.get(rule_path)
            if rule_id is None:
                missing_rule.append(rule_path)
                continue
            key = (methodology_id, rule_id, "imports")
            if key in existing:
                continue
            graph[links_key].append({
                "relation": "imports",
                "confidence": "EXTRACTED",
                "source_file": methodology_path,
                "source_location": "L1",
                "weight": 1.0,
                "_src": methodology_id,
                "_tgt": rule_id,
                "source": methodology_id,
                "target": rule_id,
                "confidence_score": 1.0,
            })
            existing.add(key)
            added += 1

    GRAPH.write_text(json.dumps(graph, indent=2) + "\n")
    log_event(
        "enrich_graph",
        "agent_end",
        added=added,
        missing_methodologies=len(missing_methodology),
        missing_rules=len(missing_rule),
    )


if __name__ == "__main__":
    main()
