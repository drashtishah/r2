"""Deterministic verifier for methbook runs.

Items 1 to 14 of the verifier checklist. Pure Python, no LLM. Items 15
to 18 are the semantic verifier's job (separate agent, separate file).
"""
from __future__ import annotations

import ast
import csv
import re
import subprocess
from pathlib import Path
from typing import Any

from methbooks.pipeline.config import ALLOWED_PATH_PREFIXES
from methbooks.pipeline.schemas.plan import Methbook

_DATAPOINTS_RE = re.compile(r"^Datapoints:[ \t]*(.*)$", re.MULTILINE)


def _result(check_id: int, passed: bool, evidence: str) -> dict[str, Any]:
    return {"id": check_id, "pass": passed, "evidence": evidence}


def _changed_paths(git_range: str) -> list[str]:
    out = subprocess.check_output(
        ["git", "diff", git_range, "--name-only"], text=True
    )
    return [p for p in out.splitlines() if p]


def _path_allowed(path: str) -> bool:
    return any(
        path.startswith(prefix) if prefix.endswith("/") else path == prefix
        for prefix in ALLOWED_PATH_PREFIXES
    )


def _parse(path: Path) -> ast.Module | None:
    try:
        return ast.parse(path.read_text())
    except (OSError, SyntaxError):
        return None


def _docstring(node: ast.AST) -> str:
    return ast.get_docstring(node) or ""  # type: ignore[arg-type]


def _datapoints(docstring: str) -> list[str]:
    m = _DATAPOINTS_RE.search(docstring)
    if not m:
        return []
    raw = m.group(1).strip().rstrip(".")
    return [d.strip() for d in raw.split(",") if d.strip()]


def _has_messaged_assert(body: list[ast.stmt]) -> tuple[bool, str]:
    asserts = [n for n in body if isinstance(n, ast.Assert)]
    if not asserts:
        return False, "no top-level assert"
    for a in asserts:
        if a.msg is None:
            return False, "assert without message"
    return True, "ok"


def _module_func(module: ast.Module, name: str) -> ast.FunctionDef | None:
    for node in module.body:
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    return None


def _read_dictionary(csv_path: Path) -> tuple[bool, list[dict[str, str]], str]:
    if not csv_path.exists():
        return False, [], f"{csv_path} not found"
    with csv_path.open() as f:
        reader = csv.DictReader(f)
        if reader.fieldnames != ["datapoint", "description", "source"]:
            return False, [], f"unexpected header: {reader.fieldnames}"
        rows = list(reader)
    return True, rows, f"{len(rows)} rows"


def run_deterministic_checks(plan: Methbook, git_range: str) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    methodology_path = Path(
        f"methbooks/methodologies/{plan.identification.provider}/{plan.identification.slug}.py"
    )
    dict_path = Path(
        f"methbooks/methodologies/{plan.identification.provider}/{plan.identification.slug}_data_dictionary.csv"
    )

    # 1. scope of git diff
    changed = _changed_paths(git_range)
    bad = [p for p in changed if not _path_allowed(p)]
    items.append(_result(
        1, not bad,
        f"out-of-scope: {bad}" if bad else f"{len(changed)} paths in scope",
    ))

    # 2. each new rule has a file
    rule_paths: dict[str, Path] = {}
    missing: list[str] = []
    for rule in plan.new_rules:
        p = Path(f"methbooks/rules/{rule.category}/{rule.name}.py")
        rule_paths[rule.name] = p
        if not p.exists():
            missing.append(str(p))
    items.append(_result(
        2, not missing,
        f"missing: {missing}" if missing else f"{len(plan.new_rules)} rule files present",
    ))

    # 3. methodology module exists
    items.append(_result(
        3, methodology_path.exists(),
        f"{methodology_path} present" if methodology_path.exists() else f"{methodology_path} missing",
    ))

    # 4. data dictionary exists with correct header
    dict_ok, dict_rows, dict_evidence = _read_dictionary(dict_path)
    items.append(_result(4, dict_ok, dict_evidence))

    # 5. each rule file ast-parses to one public def matching rule.name;
    #    private helpers (leading underscore) are allowed.
    bad_sigs: list[str] = []
    rule_modules: dict[str, ast.Module] = {}
    for rule in plan.new_rules:
        path = rule_paths[rule.name]
        if not path.exists():
            continue
        mod = _parse(path)
        if mod is None:
            bad_sigs.append(f"{path}: parse error")
            continue
        rule_modules[rule.name] = mod
        funcs = [n for n in mod.body if isinstance(n, ast.FunctionDef)]
        public = [f for f in funcs if not f.name.startswith("_")]
        if len(public) != 1:
            bad_sigs.append(f"{path}: expected 1 public def, got {len(public)}")
            continue
        fn = public[0]
        if fn.name != rule.name:
            bad_sigs.append(f"{path}: public def {fn.name} != plan {rule.name}")
            continue
        if not fn.args.args or fn.args.args[0].arg != "df":
            bad_sigs.append(f"{path}: first arg must be df")
            continue
        if fn.returns is None or "DataFrame" not in ast.unparse(fn.returns):
            bad_sigs.append(f"{path}: return annotation must reference DataFrame")
    items.append(_result(
        5, not bad_sigs,
        "; ".join(bad_sigs) if bad_sigs else "all signatures match",
    ))

    # 6. rule body contains at least one messaged assert
    bad_asserts: list[str] = []
    for rule in plan.new_rules:
        mod = rule_modules.get(rule.name)
        if mod is None:
            continue
        funcs = [
            n for n in mod.body
            if isinstance(n, ast.FunctionDef) and n.name == rule.name
        ]
        if not funcs:
            continue
        ok, why = _has_messaged_assert(funcs[0].body)
        if not ok:
            bad_asserts.append(f"{rule.name}: {why}")
    items.append(_result(
        6, not bad_asserts,
        "; ".join(bad_asserts) if bad_asserts else "all rules end with messaged assert",
    ))

    # 7-8. methodology has 3 functions; apply ends with messaged assert
    meth_module = _parse(methodology_path) if methodology_path.exists() else None
    if meth_module is None:
        items.append(_result(7, False, f"could not parse {methodology_path}"))
        items.append(_result(8, False, "methodology not parseable"))
    else:
        required = ["apply", "build_mock_data", "get_data_dictionary"]
        present = {f.name for f in meth_module.body if isinstance(f, ast.FunctionDef)}
        missing_funcs = [n for n in required if n not in present]
        items.append(_result(
            7, not missing_funcs,
            f"missing: {missing_funcs}" if missing_funcs else "all 3 functions present",
        ))
        apply_fn = _module_func(meth_module, "apply")
        if apply_fn is None:
            items.append(_result(8, False, "no apply()"))
        else:
            ok, why = _has_messaged_assert(apply_fn.body)
            items.append(_result(8, ok, why))

    # 9-10. datapoints and dictionary parity
    dict_datapoints = {row["datapoint"] for row in dict_rows}
    rule_datapoints: set[str] = set()
    for rule in plan.new_rules:
        mod = rule_modules.get(rule.name)
        if mod is None:
            continue
        for d in _datapoints(_docstring(mod)):
            rule_datapoints.add(d)
    extra = rule_datapoints - dict_datapoints
    items.append(_result(
        9, not extra,
        f"datapoints not in dictionary: {sorted(extra)}" if extra
        else f"{len(rule_datapoints)} docstring datapoints all in dictionary",
    ))
    base_columns = {"security_id", "weight"}
    unused = dict_datapoints - rule_datapoints - base_columns
    items.append(_result(
        10, not unused,
        f"dictionary rows unused: {sorted(unused)}" if unused
        else "every non-base dictionary row used by a rule",
    ))

    # 11. build_mock_data calls build_base_universe and adds non-base columns
    if meth_module is None:
        items.append(_result(11, False, "methodology not parseable"))
    else:
        bmd = _module_func(meth_module, "build_mock_data")
        if bmd is None:
            items.append(_result(11, False, "no build_mock_data()"))
        else:
            calls_base = any(
                isinstance(n, ast.Call)
                and isinstance(n.func, ast.Name)
                and n.func.id == "build_base_universe"
                for n in ast.walk(bmd)
            )
            non_base = dict_datapoints - {"security_id", "weight"}
            src = ast.unparse(bmd)
            missing_cols = {d for d in non_base if d not in src}
            ok = calls_base and not missing_cols
            reasons: list[str] = []
            if not calls_base:
                reasons.append("no build_base_universe() call")
            if missing_cols:
                reasons.append(f"missing column literals: {sorted(missing_cols)}")
            items.append(_result(
                11, ok,
                "; ".join(reasons) if reasons else "calls base and adds all non-base columns",
            ))

    # 12. make typecheck exits 0
    rc = subprocess.call(
        ["make", "typecheck"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    items.append(_result(12, rc == 0, f"make typecheck exit={rc}"))

    # 13. methodology importable and apply(build_mock_data()) works
    snippet = (
        f"from methbooks.methodologies.{plan.identification.provider}.{plan.identification.slug} "
        f"import apply, build_mock_data; apply(build_mock_data())"
    )
    rc = subprocess.call(
        ["uv", "run", "python", "-c", snippet],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    items.append(_result(13, rc == 0, f"import-and-apply exit={rc}"))

    # 14. branch has at least one rule commit plus a methodology commit;
    #     bundling related rules in a single commit is allowed.
    try:
        count_out = subprocess.check_output(
            ["git", "rev-list", "--count", git_range], text=True,
        ).strip()
        actual = int(count_out)
    except (subprocess.CalledProcessError, ValueError):
        actual = -1
    min_expected = 2  # at least one rule commit + one methodology commit
    items.append(_result(
        14, actual >= min_expected,
        f"commits: {actual} (min {min_expected})",
    ))

    overall = "pass" if all(item["pass"] for item in items) else "fail"
    return {"overall": overall, "items": items}
