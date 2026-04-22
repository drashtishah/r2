"""Snapshot tests: run each script against a fixture pair and diff the result.

Each script has different I/O semantics (per Layer 11 of the plan), so
tests are written explicitly rather than parametrized. On mismatch, the
assertion message includes the script's stderr to aid debugging.
"""
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SCRIPTS = ROOT / "scripts"
FIXTURES = Path(__file__).resolve().parent / "fixtures"


def run_script(script: str, stdin: str, cwd: Path) -> subprocess.CompletedProcess[str]:
    env = {**os.environ, "PYTHONPATH": str(ROOT)}
    return subprocess.run(
        [sys.executable, str(SCRIPTS / script)],
        input=stdin,
        capture_output=True,
        text=True,
        cwd=str(cwd),
        env=env,
    )


def test_01_sample_snapshot(tmp_path: Path) -> None:
    result = run_script("01_sample.py", "", tmp_path)
    assert result.returncode == 0, result.stderr
    expected = (FIXTURES / "01_sample.expected.json").read_text()
    assert result.stdout == expected, result.stderr


def test_02_mean_snapshot(tmp_path: Path) -> None:
    stdin = (FIXTURES / "02_mean.input.json").read_text()
    result = run_script("02_mean.py", stdin, tmp_path)
    assert result.returncode == 0, result.stderr
    actual = (tmp_path / "outputs" / "mean.txt").read_text()
    expected = (FIXTURES / "02_mean.expected.txt").read_text()
    assert actual == expected, result.stderr


def test_99_report_snapshot(tmp_path: Path) -> None:
    (tmp_path / "outputs").mkdir()
    (tmp_path / "outputs" / "mean.txt").write_text(
        (FIXTURES / "99_report.input.txt").read_text()
    )
    result = run_script("99_report.py", "", tmp_path)
    assert result.returncode == 0, result.stderr
    expected = (FIXTURES / "99_report.expected.html").read_text()
    assert result.stdout == expected, result.stderr
