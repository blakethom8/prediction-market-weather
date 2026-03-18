from pathlib import Path

from ..db import run_sql_file
from ..settings import SQL_DIR


def bootstrap() -> None:
    for path in sorted((SQL_DIR / 'ddl').glob('*.sql')):
        run_sql_file(path)
    for path in sorted((SQL_DIR / 'views').glob('*.sql')):
        run_sql_file(path)


if __name__ == '__main__':
    bootstrap()
    print('Bootstrapped DuckDB schema.')
