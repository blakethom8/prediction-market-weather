from pathlib import Path
import duckdb

from .settings import WAREHOUSE_DIR, WAREHOUSE_PATH


def connect(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(WAREHOUSE_PATH), read_only=read_only)


def run_sql_file(path: Path) -> None:
    con = connect()
    try:
        con.execute(path.read_text())
    finally:
        con.close()
