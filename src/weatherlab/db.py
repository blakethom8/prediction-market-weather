from __future__ import annotations

from pathlib import Path
import duckdb

from .settings import WAREHOUSE_DIR, WAREHOUSE_PATH


def connect(read_only: bool = False, db_path: str | Path | None = None) -> duckdb.DuckDBPyConnection:
    target = Path(db_path).expanduser() if db_path else WAREHOUSE_PATH
    target.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(target), read_only=read_only)


def run_sql_file(path: Path, db_path: str | Path | None = None) -> None:
    con = connect(db_path=db_path)
    try:
        con.execute(path.read_text())
    finally:
        con.close()
