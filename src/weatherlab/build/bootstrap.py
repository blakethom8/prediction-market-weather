from __future__ import annotations

from pathlib import Path

from ..db import run_sql_file
from ..settings import SQL_DIR


def bootstrap(db_path: str | Path | None = None) -> None:
    for path in sorted((SQL_DIR / 'ddl').glob('*.sql')):
        run_sql_file(path, db_path=db_path)
    for path in sorted((SQL_DIR / 'views').glob('*.sql')):
        run_sql_file(path, db_path=db_path)


if __name__ == '__main__':
    bootstrap()
    print('Bootstrapped DuckDB schema.')
