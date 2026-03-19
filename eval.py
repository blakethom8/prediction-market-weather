"""Compatibility wrapper for the package-owned evaluation module."""

from pathlib import Path
import sys

SRC_DIR = Path(__file__).resolve().parent / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from weatherlab.evaluation import EvalRow, main, score_row

__all__ = ["EvalRow", "score_row"]


if __name__ == "__main__":
    main()
