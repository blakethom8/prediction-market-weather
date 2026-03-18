from __future__ import annotations

from datetime import datetime, UTC

from ..db import connect
from .ids import new_id


def log_pipeline_run(job_name: str, status: str, rows_read: int = 0, rows_written: int = 0, message: str = '') -> str:
    run_id = new_id('run')
    con = connect()
    try:
        con.execute(
            '''
            insert into ops.pipeline_runs (
                run_id, job_name, started_at_utc, finished_at_utc, status, rows_read, rows_written, message
            ) values (?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            [run_id, job_name, datetime.now(UTC), datetime.now(UTC), status, rows_read, rows_written, message],
        )
    finally:
        con.close()
    return run_id
