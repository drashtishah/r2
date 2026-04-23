"""
Project-wide constants. Nothing else in the codebase should be hardcoded.
If you catch yourself typing a literal (number, path, date, string), move it here.
"""
from pathlib import Path

DATA_DIR: Path = Path("data")
OUTPUTS_DIR: Path = Path("outputs")
LOGS_DIR: Path = Path("logs")

RANDOM_SEED: int = 42
SAMPLE_SIZE: int = 100
MEAN_PRECISION: int = 9
