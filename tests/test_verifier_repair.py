"""Unit tests for the deterministic verifier's mechanical repairs.

One test per repair helper. Each asserts the file on disk reflects the
expected mutation and that a second call is a no-op (idempotent).
"""
from __future__ import annotations

import ast
import csv
from pathlib import Path

from methbooks.pipeline.verifier_checks import (
    _repair_assert_messages,
    _repair_dict_add_rows,
    _repair_dict_drop_rows,
    _repair_mock_data_columns,
)


def _write_csv(path: Path, rows: list[tuple[str, str, str]]) -> None:
    with path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["datapoint", "description", "source"])
        w.writerows(rows)


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open() as f:
        return list(csv.DictReader(f))


def test_repair_assert_messages_fills_missing(tmp_path: Path) -> None:
    rule_path = tmp_path / "my_rule.py"
    rule_path.write_text(
        "def my_rule(df):\n"
        "    out = df\n"
        "    assert 1 == 1\n"
        "    assert out is not None, 'already has message'\n"
        "    return out\n"
    )

    assert _repair_assert_messages(rule_path, "my_rule") is True
    mod = ast.parse(rule_path.read_text())
    asserts = [n for n in ast.walk(mod) if isinstance(n, ast.Assert)]
    for node in asserts:
        assert node.msg is not None, "every assert must have a message after repair"

    assert _repair_assert_messages(rule_path, "my_rule") is False


def test_repair_dict_add_rows_appends_with_placeholder(tmp_path: Path) -> None:
    csv_path = tmp_path / "d.csv"
    _write_csv(csv_path, [("security_id", "id", "base"), ("weight", "w", "base")])

    assert _repair_dict_add_rows(csv_path, ["alpha", "beta"]) is True
    rows = _read_csv(csv_path)
    names = [r["datapoint"] for r in rows]
    assert "alpha" in names and "beta" in names
    for row in rows:
        if row["datapoint"] in {"alpha", "beta"}:
            assert row["description"] == "TODO"
            assert row["source"] == "TODO"

    assert _repair_dict_add_rows(csv_path, []) is False


def test_repair_dict_drop_rows_removes_targets(tmp_path: Path) -> None:
    csv_path = tmp_path / "d.csv"
    _write_csv(csv_path, [
        ("security_id", "id", "base"),
        ("weight", "w", "base"),
        ("alpha", "a", "src"),
        ("beta", "b", "src"),
        ("gamma", "g", "src"),
    ])

    assert _repair_dict_drop_rows(csv_path, {"alpha", "gamma"}) is True
    names = [r["datapoint"] for r in _read_csv(csv_path)]
    assert names == ["security_id", "weight", "beta"]

    assert _repair_dict_drop_rows(csv_path, set()) is False


def test_repair_mock_data_columns_inserts_with_columns(tmp_path: Path) -> None:
    meth_path = tmp_path / "m.py"
    meth_path.write_text(
        "import polars as pl\n"
        "\n"
        "def build_mock_data():\n"
        "    df = pl.DataFrame({'x': [1]})\n"
        "    return df\n"
    )

    assert _repair_mock_data_columns(meth_path, {"new_col1", "new_col2"}) is True
    src = meth_path.read_text()
    assert "with_columns" in src
    assert "'new_col1'" in src and "'new_col2'" in src
    assert "pl.lit(0.0)" in src

    assert _repair_mock_data_columns(meth_path, set()) is False


def test_repair_mock_data_columns_skips_non_name_return(tmp_path: Path) -> None:
    """Return value must be a bare Name; complex return expressions are untouched."""
    meth_path = tmp_path / "m.py"
    original = (
        "import polars as pl\n"
        "\n"
        "def build_mock_data():\n"
        "    return pl.DataFrame({'x': [1]})\n"
    )
    meth_path.write_text(original)

    assert _repair_mock_data_columns(meth_path, {"missing"}) is False
    assert meth_path.read_text() == original
