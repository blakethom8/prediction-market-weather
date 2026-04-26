from __future__ import annotations

from datetime import UTC, datetime
import os
from pathlib import Path
import subprocess
from typing import Any, Callable, Sequence

from ..db import connect

_REPO_ROOT = Path(__file__).resolve().parents[3]
_DEFAULT_REVIEWS_DIR = _REPO_ROOT / 'self-improvement' / 'reviews'
_DEFAULT_COMMAND = ('make', 'chief', '--', 'calibration')
_INTERVAL = 10

CommandRunner = Callable[..., subprocess.CompletedProcess[str]]


def get_settled_bet_count(db_path: str | Path | None = None) -> int:
    """Count paper bets that have settled outcomes available for calibration."""

    con = connect(read_only=True, db_path=db_path)
    try:
        table_exists = con.execute(
            '''
            select count(*)
            from information_schema.tables
            where table_schema = 'ops'
              and table_name = 'paper_bets'
            '''
        ).fetchone()
        if not table_exists or not table_exists[0]:
            return 0
        row = con.execute(
            '''
            select count(*)
            from ops.paper_bets
            where status in ('closed', 'settled')
              and outcome_label is not null
            '''
        ).fetchone()
        return int(row[0] or 0)
    finally:
        con.close()


def crossed_calibration_thresholds(before_count: int, after_count: int) -> list[int]:
    if after_count < _INTERVAL or after_count <= before_count:
        return []

    first = (before_count // _INTERVAL) + 1
    last = after_count // _INTERVAL
    return [value * _INTERVAL for value in range(first, last + 1)]


def maybe_write_calibration_reviews(
    *,
    before_count: int,
    after_count: int,
    reviews_dir: str | Path | None = None,
    repo_root: str | Path | None = None,
    command: Sequence[str] = _DEFAULT_COMMAND,
    runner: CommandRunner = subprocess.run,
    now: datetime | None = None,
) -> list[Path]:
    """Run Chief calibration and write review files for newly crossed thresholds."""

    thresholds = crossed_calibration_thresholds(before_count, after_count)
    if not thresholds:
        return []

    timestamp = now or datetime.now(UTC)
    root = Path(repo_root).expanduser() if repo_root is not None else _REPO_ROOT
    target_dir = Path(reviews_dir).expanduser() if reviews_dir is not None else _DEFAULT_REVIEWS_DIR
    target_dir.mkdir(parents=True, exist_ok=True)

    completed = _run_calibration_command(command=command, runner=runner, repo_root=root)
    written_paths: list[Path] = []
    for threshold in thresholds:
        path = _next_review_path(target_dir, timestamp, threshold)
        path.write_text(
            _format_review(
                timestamp=timestamp,
                threshold=threshold,
                before_count=before_count,
                after_count=after_count,
                command=command,
                completed=completed,
            )
        )
        written_paths.append(path)
    return written_paths


def _run_calibration_command(
    *,
    command: Sequence[str],
    runner: CommandRunner,
    repo_root: Path,
) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env.setdefault('PYTHONPATH', 'src')
    try:
        return runner(
            list(command),
            cwd=str(repo_root),
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )
    except Exception as exc:
        return subprocess.CompletedProcess(
            args=list(command),
            returncode=1,
            stdout='',
            stderr=f'{type(exc).__name__}: {exc}',
        )


def _next_review_path(reviews_dir: Path, timestamp: datetime, threshold: int) -> Path:
    date_prefix = timestamp.date().isoformat()
    base = reviews_dir / f'{date_prefix}-calibration-{threshold:03d}-settled-bets.md'
    if not base.exists():
        return base

    suffix = timestamp.strftime('%H%M%S')
    candidate = reviews_dir / f'{date_prefix}-calibration-{threshold:03d}-settled-bets-{suffix}.md'
    index = 2
    while candidate.exists():
        candidate = reviews_dir / f'{date_prefix}-calibration-{threshold:03d}-settled-bets-{suffix}-{index}.md'
        index += 1
    return candidate


def _format_review(
    *,
    timestamp: datetime,
    threshold: int,
    before_count: int,
    after_count: int,
    command: Sequence[str],
    completed: subprocess.CompletedProcess[str],
) -> str:
    status = 'completed' if int(completed.returncode) == 0 else f'failed ({completed.returncode})'
    command_label = ' '.join(command)
    stdout = _strip_output(completed.stdout)
    stderr = _strip_output(completed.stderr)

    lines: list[str] = [
        f'# Calibration Review - {threshold} Settled Bets',
        '',
        f'- Generated at: {timestamp.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")}',
        f'- Trigger: settled bet count crossed {threshold} ({before_count} -> {after_count})',
        f'- Command: `{command_label}`',
        f'- Status: {status}',
        '',
        '## Calibration Output',
        '',
        '```text',
        stdout or '[no stdout]',
        '```',
    ]
    if stderr:
        lines.extend([
            '',
            '## Command Stderr',
            '',
            '```text',
            stderr,
            '```',
        ])
    return '\n'.join(lines).rstrip() + '\n'


def _strip_output(value: Any) -> str:
    if value is None:
        return ''
    return str(value).strip()
