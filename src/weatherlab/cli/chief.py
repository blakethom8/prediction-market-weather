"""Chief CLI — AI-readable operations interface for the prediction market system.

Usage:
    PYTHONPATH=src python -m weatherlab.cli.chief <command> [options]
    make chief -- <command> [options]

Commands:
    status                  Daily operations briefing
    board [--date DATE]     Daily market board
    bets [--status STATUS] [--limit N]  List paper bets
    calibration [--city CITY] [--days N]  Model accuracy report
    sync                    Run live data sync
    settle                  Check open bets for settlement
    killswitch [on|off]     Toggle the kill switch
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

logging.basicConfig(level=logging.WARNING, format="%(levelname)s: %(message)s")

_REPO_ROOT = Path(__file__).resolve().parents[4]
_CONFIG_PATH = _REPO_ROOT / "config" / "chief_state.json"


# ---------------------------------------------------------------------------
# Kill switch state
# ---------------------------------------------------------------------------

def _load_state() -> dict[str, Any]:
    if _CONFIG_PATH.exists():
        try:
            return json.loads(_CONFIG_PATH.read_text())
        except Exception:
            pass
    return {"kill_switch": True, "updated_at": None, "updated_by": "chief-cli"}


def _save_state(state: dict[str, Any]) -> None:
    _CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    _CONFIG_PATH.write_text(json.dumps(state, indent=2) + "\n")


def _get_kill_switch() -> bool:
    return bool(_load_state().get("kill_switch", True))


# ---------------------------------------------------------------------------
# Command handlers
# ---------------------------------------------------------------------------

def cmd_status(args: argparse.Namespace) -> None:
    from .queries import get_sync_status, get_board_rows, get_open_bets, get_pnl_summary
    from .formatters import format_status

    today = date.today().isoformat()
    sync_status = get_sync_status()
    board_rows = get_board_rows(target_date=date.today())
    open_bets = get_open_bets()
    pnl = get_pnl_summary()
    kill_switch = _get_kill_switch()

    print(format_status(sync_status, board_rows, open_bets, pnl, kill_switch, today))


def cmd_board(args: argparse.Namespace) -> None:
    from .queries import get_board_rows
    from .formatters import format_board

    if args.date:
        try:
            target = date.fromisoformat(args.date)
        except ValueError:
            print(f"Invalid date format: {args.date}  (expected YYYY-MM-DD)", file=sys.stderr)
            sys.exit(1)
    else:
        target = date.today()

    board_rows = get_board_rows(target_date=target)
    print(format_board(board_rows, target.isoformat()))


def cmd_bets(args: argparse.Namespace) -> None:
    from .queries import get_bets
    from .formatters import format_bets

    status = args.status or "open"
    limit = args.limit or 20
    bets = get_bets(status=status, limit=limit)
    print(format_bets(bets, status))


def cmd_calibration(args: argparse.Namespace) -> None:
    from .queries import get_calibration_by_city, get_calibration_by_edge_band
    from .formatters import format_calibration

    days = args.days or 90
    city = args.city or None
    by_city = get_calibration_by_city(days=days, city=city)
    by_edge = get_calibration_by_edge_band(days=days)
    print(format_calibration(by_city, by_edge, days))


def cmd_sync(args: argparse.Namespace) -> None:
    from .sync import run_full_sync
    from .formatters import format_sync_result

    print("Starting sync...", flush=True)
    result = run_full_sync()
    print(format_sync_result(result))


def cmd_settle(args: argparse.Namespace) -> None:
    from .settle import check_and_settle_open_bets
    from .formatters import format_settle_result

    results = check_and_settle_open_bets()
    settled_count = sum(1 for r in results if r.get("settled"))
    open_count = len(results) - settled_count
    print(format_settle_result(results, settled_count, open_count))


def cmd_killswitch(args: argparse.Namespace) -> None:
    from .formatters import format_killswitch, format_killswitch_status

    state = _load_state()
    old_value = bool(state.get("kill_switch", True))

    value_arg = args.value

    if value_arg is None:
        # No argument: just show current state
        print(format_killswitch_status(old_value))
        return

    if value_arg.lower() == "on":
        new_value = True
    elif value_arg.lower() == "off":
        new_value = False
    else:
        print(f"Invalid value '{value_arg}' — use 'on' or 'off'", file=sys.stderr)
        sys.exit(1)

    state["kill_switch"] = new_value
    state["updated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    state["updated_by"] = "chief-cli"
    _save_state(state)

    print(format_killswitch(old_value, new_value))


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="chief",
        description="Chief CLI — AI-readable prediction market operations interface",
    )
    subparsers = parser.add_subparsers(dest="command", metavar="COMMAND")

    # status
    subparsers.add_parser("status", help="Daily operations briefing")

    # board
    board_p = subparsers.add_parser("board", help="Daily market board")
    board_p.add_argument("--date", metavar="YYYY-MM-DD", help="Target date (default: today)")

    # bets
    bets_p = subparsers.add_parser("bets", help="List paper bets")
    bets_p.add_argument(
        "--status",
        choices=["open", "settled", "all"],
        default="open",
        help="Filter by status (default: open)",
    )
    bets_p.add_argument("--limit", type=int, default=20, metavar="N", help="Max rows (default: 20)")

    # calibration
    cal_p = subparsers.add_parser("calibration", help="Model accuracy report")
    cal_p.add_argument("--city", metavar="CITY", help="Filter to a single city_id")
    cal_p.add_argument("--days", type=int, default=90, metavar="N", help="Look-back window in days (default: 90)")

    # sync
    subparsers.add_parser("sync", help="Run live Kalshi + forecast sync")

    # settle
    subparsers.add_parser("settle", help="Check open bets for settlement")

    # killswitch
    ks_p = subparsers.add_parser("killswitch", help="Toggle kill switch")
    ks_p.add_argument("value", nargs="?", choices=["on", "off"], help="on | off (omit to show current state)")

    return parser


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(1)

    handlers = {
        "status": cmd_status,
        "board": cmd_board,
        "bets": cmd_bets,
        "calibration": cmd_calibration,
        "sync": cmd_sync,
        "settle": cmd_settle,
        "killswitch": cmd_killswitch,
    }

    handler = handlers.get(args.command)
    if handler is None:
        print(f"Unknown command: {args.command}", file=sys.stderr)
        sys.exit(1)

    try:
        handler(args)
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        sys.exit(130)
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        if "--debug" in sys.argv:
            raise
        sys.exit(1)


if __name__ == "__main__":
    main()
