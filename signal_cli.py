"""Compatibility wrapper for the package-owned signal module.

This lives under a non-stdlib-clashing name so repo-root app launches do not
shadow Python's built-in ``signal`` module.
"""

from pathlib import Path
import sys

SRC_DIR = Path(__file__).resolve().parent / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from weatherlab.signal import choose_action, compute_edge, main

__all__ = ["choose_action", "compute_edge"]


if __name__ == "__main__":
    main()
