"""All DuckDB queries for the Chief CLI.

Returns dicts/lists-of-dicts — never dataframes.
Handles missing tables gracefully (returns empty structures, not exceptions).
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from ..db import connect


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _con(db_path: str | Path | None = None):
    # In-memory databases cannot be opened read-only
    if db_path is not None and str(db_path) == ":memory:":
        return connect(read_only=False, db_path=db_path)
    return connect(read_only=True, db_path=db_path)


def _table_exists(con, schema: str, table: str) -> bool:
    try:
        row = con.execute(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_schema = ? AND table_name = ?",
            [schema, table],
        ).fetchone()
        return bool(row and row[0])
    except Exception:
        return False


def _fetchall_as_dicts(con, sql: str, params: list | None = None) -> list[dict[str, Any]]:
    cursor = con.execute(sql, params or [])
    columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


# ---------------------------------------------------------------------------
# Sync status
# ---------------------------------------------------------------------------

def get_sync_status(db_path: str | Path | None = None) -> dict[str, Any]:
    """Return last Kalshi sync time and last forecast sync time."""
    result: dict[str, Any] = {
        "last_kalshi_ts": None,
        "last_forecast_ts": None,
    }
    con = _con(db_path)
    try:
        if _table_exists(con, "core", "market_snapshots"):
            row = con.execute(
                "SELECT max(ts_utc) FROM core.market_snapshots"
            ).fetchone()
            result["last_kalshi_ts"] = row[0] if row else None

        if _table_exists(con, "core", "forecast_snapshots"):
            row = con.execute(
                "SELECT max(available_at_utc) FROM core.forecast_snapshots"
            ).fetchone()
            result["last_forecast_ts"] = row[0] if row else None
    except Exception as exc:
        result["error"] = str(exc)
    finally:
        con.close()
    return result


# ---------------------------------------------------------------------------
# Board
# ---------------------------------------------------------------------------

def get_board_rows(
    target_date: date | None = None,
    db_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    """Return all board rows from features.v_daily_market_board for a date."""
    if target_date is None:
        target_date = datetime.now(timezone.utc).date()

    con = _con(db_path)
    try:
        if not _table_exists(con, "features", "v_daily_market_board"):
            return []
        rows = _fetchall_as_dicts(
            con,
            """
            SELECT
                market_ticker,
                market_title,
                city_id,
                market_date_local,
                minutes_to_close,
                price_yes_mid,
                price_yes_ask,
                price_yes_bid,
                fair_prob,
                edge_vs_mid,
                edge_vs_ask,
                candidate_bucket,
                candidate_rank
            FROM features.v_daily_market_board
            WHERE market_date_local = ?
            ORDER BY candidate_rank ASC NULLS LAST, edge_vs_ask DESC NULLS LAST
            """,
            [target_date],
        )
    except Exception:
        rows = []
    finally:
        con.close()
    return rows


# ---------------------------------------------------------------------------
# Bets
# ---------------------------------------------------------------------------

def get_bets(
    status: str = "open",
    limit: int = 20,
    db_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    """Return paper bets filtered by status."""
    con = _con(db_path)
    try:
        if not _table_exists(con, "ops", "paper_bets"):
            return []

        if status == "all":
            where_clause = ""
            params: list[Any] = []
        else:
            where_clause = "WHERE pb.status = ?"
            params = [status]

        rows = _fetchall_as_dicts(
            con,
            f"""
            SELECT
                pb.paper_bet_id,
                pb.market_ticker,
                pb.side,
                pb.notional_dollars,
                pb.expected_edge,
                pb.status,
                pb.created_at_utc,
                pb.closed_at_utc,
                pb.outcome_label,
                pb.realized_pnl,
                pb.strategy_variant,
                wc.city_id
            FROM ops.paper_bets pb
            LEFT JOIN core.weather_contracts wc ON pb.market_ticker = wc.market_ticker
            {where_clause}
            ORDER BY pb.created_at_utc DESC
            LIMIT ?
            """,
            params + [limit],
        )
    except Exception:
        rows = []
    finally:
        con.close()
    return rows


def get_open_bets(db_path: str | Path | None = None) -> list[dict[str, Any]]:
    return get_bets(status="open", limit=200, db_path=db_path)


# ---------------------------------------------------------------------------
# P&L summary
# ---------------------------------------------------------------------------

def get_pnl_summary(db_path: str | Path | None = None) -> dict[str, Any]:
    """Return aggregate P&L stats across all paper bets."""
    result: dict[str, Any] = {
        "total_settled": 0,
        "wins": 0,
        "losses": 0,
        "realized_pnl": None,
        "open_count": 0,
        "open_notional": None,
    }
    con = _con(db_path)
    try:
        if not _table_exists(con, "ops", "paper_bets"):
            return result

        row = con.execute(
            """
            SELECT
                count(*) FILTER (WHERE status = 'closed') AS total_settled,
                count(*) FILTER (
                    WHERE status = 'closed'
                      AND outcome_label IS NOT NULL
                      AND (
                        (side = 'BUY_YES' AND outcome_label = 'YES')
                        OR (side = 'BUY_NO' AND outcome_label = 'NO')
                      )
                ) AS wins,
                count(*) FILTER (
                    WHERE status = 'closed'
                      AND outcome_label IS NOT NULL
                      AND NOT (
                        (side = 'BUY_YES' AND outcome_label = 'YES')
                        OR (side = 'BUY_NO' AND outcome_label = 'NO')
                      )
                ) AS losses,
                sum(realized_pnl) FILTER (WHERE status = 'closed') AS realized_pnl,
                count(*) FILTER (WHERE status = 'open') AS open_count,
                sum(notional_dollars) FILTER (WHERE status = 'open') AS open_notional
            FROM ops.paper_bets
            """
        ).fetchone()
        if row:
            result["total_settled"] = int(row[0] or 0)
            result["wins"] = int(row[1] or 0)
            result["losses"] = int(row[2] or 0)
            result["realized_pnl"] = float(row[3]) if row[3] is not None else None
            result["open_count"] = int(row[4] or 0)
            result["open_notional"] = float(row[5]) if row[5] is not None else None
    except Exception as exc:
        result["error"] = str(exc)
    finally:
        con.close()
    return result


# ---------------------------------------------------------------------------
# Calibration
# ---------------------------------------------------------------------------

def get_calibration_by_city(
    days: int = 90,
    city: str | None = None,
    db_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    """Return calibration stats broken down by city for settled bets."""
    con = _con(db_path)
    try:
        if not _table_exists(con, "ops", "paper_bets"):
            return []

        city_filter = ""
        params: list[Any] = [days]
        if city:
            city_filter = "AND lower(wc.city_id) = lower(?)"
            params.append(city)

        rows = _fetchall_as_dicts(
            con,
            f"""
            SELECT
                wc.city_id,
                count(*) AS bet_count,
                avg(
                    CASE
                        WHEN (pb.side = 'BUY_YES' AND pb.outcome_label = 'YES')
                          OR (pb.side = 'BUY_NO'  AND pb.outcome_label = 'NO')
                        THEN 1.0 ELSE 0.0
                    END
                ) AS win_rate,
                avg(pb.expected_edge) AS avg_edge,
                avg(pb.realized_pnl)  AS avg_pnl,
                sum(pb.realized_pnl)  AS total_pnl
            FROM ops.paper_bets pb
            JOIN core.weather_contracts wc ON pb.market_ticker = wc.market_ticker
            WHERE pb.status = 'closed'
              AND pb.outcome_label IS NOT NULL
              AND pb.created_at_utc >= now() - INTERVAL (? || ' days')
              {city_filter}
            GROUP BY wc.city_id
            ORDER BY avg_pnl ASC
            """,
            params,
        )
    except Exception:
        rows = []
    finally:
        con.close()
    return rows


def get_calibration_by_edge_band(
    days: int = 90,
    db_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    """Return calibration stats broken down by expected_edge bands."""
    con = _con(db_path)
    try:
        if not _table_exists(con, "ops", "paper_bets"):
            return []

        rows = _fetchall_as_dicts(
            con,
            """
            WITH bets AS (
                SELECT
                    pb.expected_edge,
                    pb.realized_pnl,
                    CASE
                        WHEN (pb.side = 'BUY_YES' AND pb.outcome_label = 'YES')
                          OR (pb.side = 'BUY_NO'  AND pb.outcome_label = 'NO')
                        THEN 1 ELSE 0
                    END AS won
                FROM ops.paper_bets pb
                WHERE pb.status = 'closed'
                  AND pb.outcome_label IS NOT NULL
                  AND pb.created_at_utc >= now() - INTERVAL (? || ' days')
                  AND pb.expected_edge IS NOT NULL
            ),
            banded AS (
                SELECT
                    CASE
                        WHEN expected_edge < 0.05  THEN 'below_0.05'
                        WHEN expected_edge < 0.10  THEN '0.05-0.10'
                        WHEN expected_edge < 0.20  THEN '0.10-0.20'
                        ELSE '0.20+'
                    END AS edge_band,
                    CASE
                        WHEN expected_edge < 0.05  THEN 0
                        WHEN expected_edge < 0.10  THEN 1
                        WHEN expected_edge < 0.20  THEN 2
                        ELSE 3
                    END AS band_order,
                    won,
                    realized_pnl,
                    expected_edge
                FROM bets
            )
            SELECT
                edge_band,
                count(*) AS bet_count,
                avg(won::double) AS win_rate,
                avg(LEAST(1.0, GREATEST(0.0, 0.5 + expected_edge))) AS expected_win_rate,
                avg(expected_edge) AS avg_edge,
                sum(realized_pnl) AS total_pnl
            FROM banded
            GROUP BY edge_band, band_order
            ORDER BY band_order
            """,
            [days],
        )
    except Exception:
        rows = []
    finally:
        con.close()
    return rows


# ---------------------------------------------------------------------------
# Settlement data lookup
# ---------------------------------------------------------------------------

def get_open_bets_with_contracts(
    db_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    """Return open bets joined with contract details needed for settlement."""
    con = _con(db_path)
    try:
        if not _table_exists(con, "ops", "paper_bets"):
            return []

        rows = _fetchall_as_dicts(
            con,
            """
            SELECT
                pb.paper_bet_id,
                pb.market_ticker,
                pb.side,
                pb.limit_price,
                pb.quantity,
                pb.notional_dollars,
                pb.expected_edge,
                pb.created_at_utc,
                wc.city_id,
                wc.station_id,
                wc.market_date_local,
                wc.measure,
                wc.operator,
                wc.threshold_low_f,
                wc.threshold_high_f
            FROM ops.paper_bets pb
            JOIN core.weather_contracts wc ON pb.market_ticker = wc.market_ticker
            WHERE pb.status = 'open'
            ORDER BY pb.created_at_utc DESC
            """,
        )
    except Exception:
        rows = []
    finally:
        con.close()
    return rows


def get_settlement_observation(
    station_id: str,
    market_date_local: date,
    db_path: str | Path | None = None,
) -> dict[str, Any] | None:
    """Return best available settlement observation for a station + date."""
    con = _con(db_path)
    try:
        if not _table_exists(con, "core", "settlement_observations"):
            return None

        # Prefer official (nws-cli) over kalshi-implied
        row = con.execute(
            """
            SELECT
                settlement_id,
                source,
                station_id,
                city_id,
                market_date_local,
                observed_high_temp_f,
                observed_low_temp_f,
                is_final
            FROM core.settlement_observations
            WHERE station_id = ?
              AND market_date_local = ?
            ORDER BY
                CASE source WHEN 'nws-cli' THEN 0 ELSE 1 END,
                report_published_at_utc DESC NULLS LAST
            LIMIT 1
            """,
            [station_id, market_date_local],
        ).fetchone()
        if row is None:
            return None
        cols = [
            "settlement_id", "source", "station_id", "city_id",
            "market_date_local", "observed_high_temp_f", "observed_low_temp_f", "is_final",
        ]
        return dict(zip(cols, row))
    except Exception:
        return None
    finally:
        con.close()


# ---------------------------------------------------------------------------
# Pipeline runs (for sync timestamps)
# ---------------------------------------------------------------------------

def get_pipeline_run_summary(db_path: str | Path | None = None) -> list[dict[str, Any]]:
    """Return the most recent pipeline run for each job_name."""
    con = _con(db_path)
    try:
        if not _table_exists(con, "ops", "pipeline_runs"):
            return []
        rows = _fetchall_as_dicts(
            con,
            """
            SELECT DISTINCT ON (job_name)
                job_name,
                started_at_utc,
                finished_at_utc,
                status,
                rows_written,
                message
            FROM ops.pipeline_runs
            ORDER BY job_name, started_at_utc DESC
            """,
        )
    except Exception:
        rows = []
    finally:
        con.close()
    return rows
