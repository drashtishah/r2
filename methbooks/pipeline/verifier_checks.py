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


def _repair_assert_messages(rule_path: Path, rule_name: str) -> bool:
    """Add a literal message to every bare `assert X` in rule_path.

    Does not invent asserts; only fills missing messages with
    `f"{rule_name}: assertion failed"`. Returns True if the file changed.
    """
    mod = _parse(rule_path)
    if mod is None:
        return False
    changed = False
    for node in ast.walk(mod):
        if isinstance(node, ast.Assert) and node.msg is None:
            node.msg = ast.Constant(value=f"{rule_name}: assertion failed")
            changed = True
    if changed:
        ast.fix_missing_locations(mod)
        rule_path.write_text(ast.unparse(mod))
    return changed


def _repair_dict_add_rows(csv_path: Path, missing: list[str]) -> bool:
    if not missing:
        return False
    with csv_path.open("a", newline="") as f:
        writer = csv.writer(f)
        for d in sorted(missing):
            writer.writerow([d, "TODO", "TODO"])
    return True


def _repair_dict_drop_rows(csv_path: Path, unused: set[str]) -> bool:
    if not unused:
        return False
    with csv_path.open() as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames
        rows = [row for row in reader if row["datapoint"] not in unused]
    with csv_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)  # type: ignore[arg-type]
        writer.writeheader()
        writer.writerows(rows)
    return True


def _repair_mock_data_columns(meth_path: Path, missing_cols: set[str]) -> bool:
    """Insert `<var> = <var>.with_columns(pl.lit(0.0).alias("<col>"), ...)`
    immediately before the last `return <var>` statement of build_mock_data.
    Only safe when the return value is a bare Name; other shapes are left alone.
    """
    if not missing_cols:
        return False
    mod = _parse(meth_path)
    if mod is None:
        return False
    bmd = _module_func(mod, "build_mock_data")
    if bmd is None:
        return False
    ret_idx: int | None = None
    for i, stmt in enumerate(bmd.body):
        if isinstance(stmt, ast.Return):
            ret_idx = i
    if ret_idx is None:
        return False
    ret = bmd.body[ret_idx]
    assert isinstance(ret, ast.Return)
    if not isinstance(ret.value, ast.Name):
        return False
    var = ret.value.id
    aliases = [
        ast.Call(
            func=ast.Attribute(
                value=ast.Call(
                    func=ast.Attribute(value=ast.Name(id="pl"), attr="lit"),
                    args=[ast.Constant(value=0.0)],
                    keywords=[],
                ),
                attr="alias",
            ),
            args=[ast.Constant(value=col)],
            keywords=[],
        )
        for col in sorted(missing_cols)
    ]
    assign = ast.Assign(
        targets=[ast.Name(id=var)],
        value=ast.Call(
            func=ast.Attribute(value=ast.Name(id=var), attr="with_columns"),
            args=aliases,  # type: ignore[arg-type]
            keywords=[],
        ),
    )
    bmd.body.insert(ret_idx, assign)
    ast.fix_missing_locations(mod)
    meth_path.write_text(ast.unparse(mod))
    return True


def _apply_mechanical_repairs(
    plan: Methbook,
    dict_path: Path,
    methodology_path: Path,
    rule_paths: dict[str, Path],
) -> bool:
    """Apply all deterministic repairs. Returns True if any file changed.

    Order: CSV row add/drop first (changes the dictionary set), then
    build_mock_data column inserts (which read the post-repair dict),
    then rule assert messages (independent of the others).
    """
    changed = False

    csv_ok, dict_rows, _ = _read_dictionary(dict_path)
    dict_datapoints = {r["datapoint"] for r in dict_rows} if csv_ok else set()

    rule_datapoints: set[str] = set()
    for rule in plan.new_rules:
        p = rule_paths.get(rule.name)
        if p is None or not p.exists():
            continue
        mod = _parse(p)
        if mod is None:
            continue
        for d in _datapoints(_docstring(mod)):
            rule_datapoints.add(d)

    if csv_ok:
        missing = sorted(rule_datapoints - dict_datapoints)
        if missing and _repair_dict_add_rows(dict_path, missing):
            changed = True
        base_columns = {"security_id", "weight"}
        unused = dict_datapoints - rule_datapoints - base_columns
        if unused and _repair_dict_drop_rows(dict_path, unused):
            changed = True

    csv_ok, dict_rows, _ = _read_dictionary(dict_path)
    dict_datapoints = {r["datapoint"] for r in dict_rows} if csv_ok else set()

    if methodology_path.exists():
        meth_module = _parse(methodology_path)
        if meth_module is not None:
            bmd = _module_func(meth_module, "build_mock_data")
            if bmd is not None:
                non_base = dict_datapoints - {"security_id", "weight"}
                src = ast.unparse(bmd)
                missing_cols = {d for d in non_base if d not in src}
                if missing_cols and _repair_mock_data_columns(
                    methodology_path, missing_cols,
                ):
                    changed = True

    for rule in plan.new_rules:
        p = rule_paths.get(rule.name)
        if p is None or not p.exists():
            continue
        mod = _parse(p)
        if mod is None:
            continue
        funcs = [
            n for n in mod.body
            if isinstance(n, ast.FunctionDef) and n.name == rule.name
        ]
        if not funcs:
            continue
        ok, why = _has_messaged_assert(funcs[0].body)
        if not ok and why == "assert without message":
            if _repair_assert_messages(p, rule.name):
                changed = True

    return changed


def _commit_repairs() -> None:
    subprocess.check_call(["git", "add", "-A", "methbooks/"])
    if subprocess.call(
        ["git", "diff", "--cached", "--quiet"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    ) != 0:
        subprocess.check_call(
            ["git", "commit", "-m", "verifier: auto-repair mechanical drift"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )


def run_deterministic_checks(plan: Methbook, git_range: str) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    methodology_path = Path(
        f"methbooks/methodologies/{plan.identification.provider}/{plan.identification.slug}.py"
    )
    dict_path = Path(
        f"methbooks/methodologies/{plan.identification.provider}/{plan.identification.slug}_data_dictionary.csv"
    )
    rule_paths: dict[str, Path] = {
        rule.name: Path(f"methbooks/rules/{rule.category}/{rule.name}.py")
        for rule in plan.new_rules
    }

    if _apply_mechanical_repairs(plan, dict_path, methodology_path, rule_paths):
        _commit_repairs()

    # 1. scope of git diff
    changed = _changed_paths(git_range)
    bad = [p for p in changed if not _path_allowed(p)]
    items.append(_result(
        1, not bad,
        f"out-of-scope: {bad}" if bad else f"{len(changed)} paths in scope",
    ))

    # 2. each new rule has a file
    missing = [str(p) for p in rule_paths.values() if not p.exists()]
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

    # 19. page consistency: if any rule cites a page, every rule with a
    #     source.line must also have one. Catches commit_plan regressions
    #     on footered docs without flagging footer-less docs (all None).
    cited = [r for r in plan.new_rules if r.source and r.source.line is not None]
    if cited and any(r.source.page is not None for r in cited):
        missing = [r.name for r in cited if r.source.page is None]
        items.append(_result(
            19, not missing,
            f"rules missing page: {missing}" if missing
            else f"page set on all {len(cited)} cited rules",
        ))
    else:
        items.append(_result(
            19, True,
            "no pages cited (source markdown has no page footers)" if cited
            else "no rule citations to check",
        ))

    overall = "pass" if all(item["pass"] for item in items) else "fail"
    return {"overall": overall, "items": items}
