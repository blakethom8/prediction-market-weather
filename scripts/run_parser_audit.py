#!/usr/bin/env python3
"""Batch-audit contract titles with the weather contract parser.

Usage:
    PYTHONPATH=src python3 scripts/run_parser_audit.py <input.csv> [output.json]
"""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from weatherlab.parse.audit import audit_titles, summarize_audit


def main() -> int:
    if len(sys.argv) < 2:
        print('Usage: run_parser_audit.py <input.csv> [output.json]')
        return 1

    input_path = Path(sys.argv[1])
    rows = []
    with input_path.open('r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows.extend(reader)

    audited = audit_titles(rows)
    summary = summarize_audit(audited)
    print(json.dumps(summary, indent=2, default=str))

    if len(sys.argv) >= 3:
        output_path = Path(sys.argv[2])
        output_path.write_text(json.dumps(audited, indent=2, default=str), encoding='utf-8')

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
