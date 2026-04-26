"""Unit tests for chief CLI query functions.

These tests run against the real DuckDB warehouse. They document the expected
shape of each query result and verify graceful degradation on empty/missing data.
"""
from __future__ import annotations

import os
import unittest
from datetime import date, datetime, timezone
from pathlib import Path

# Use env var or default warehouse path
_WAREHOUSE_PATH = os.environ.get(
    "WEATHER_WAREHOUSE_PATH",
    str(Path(__file__).resolve().parents[1] / "data" / "warehouse" / "weather_markets.duckdb"),
)


class TestGetSyncStatus(unittest.TestCase):
    def test_returns_dict_with_expected_keys(self):
        from weatherlab.cli.queries import get_sync_status

        result = get_sync_status(db_path=_WAREHOUSE_PATH)
        self.assertIsInstance(result, dict)
        self.assertIn("last_kalshi_ts", result)
        self.assertIn("last_forecast_ts", result)

    def test_timestamps_are_datetime_or_none(self):
        from weatherlab.cli.queries import get_sync_status

        result = get_sync_status(db_path=_WAREHOUSE_PATH)
        for key in ("last_kalshi_ts", "last_forecast_ts"):
            val = result[key]
            if val is not None:
                self.assertTrue(
                    isinstance(val, datetime),
                    f"{key} should be datetime, got {type(val)}",
                )

    def test_no_crash_on_empty_db(self):
        """Should return a dict with None values rather than raising."""
        from weatherlab.cli.queries import get_sync_status

        result = get_sync_status(db_path=":memory:")
        self.assertIsInstance(result, dict)


class TestGetBoardRows(unittest.TestCase):
    def test_returns_list(self):
        from weatherlab.cli.queries import get_board_rows

        rows = get_board_rows(db_path=_WAREHOUSE_PATH)
        self.assertIsInstance(rows, list)

    def test_row_has_expected_keys(self):
        from weatherlab.cli.queries import get_board_rows

        rows = get_board_rows(db_path=_WAREHOUSE_PATH)
        if rows:
            row = rows[0]
            for key in ("market_ticker", "city_id", "fair_prob", "edge_vs_ask", "candidate_bucket"):
                self.assertIn(key, row, f"Missing key: {key}")

    def test_empty_list_on_missing_date(self):
        from weatherlab.cli.queries import get_board_rows

        rows = get_board_rows(target_date=date(2000, 1, 1), db_path=_WAREHOUSE_PATH)
        self.assertEqual(rows, [])

    def test_no_crash_on_empty_db(self):
        from weatherlab.cli.queries import get_board_rows

        rows = get_board_rows(db_path=":memory:")
        self.assertEqual(rows, [])


class TestGetBets(unittest.TestCase):
    def test_returns_list(self):
        from weatherlab.cli.queries import get_bets

        rows = get_bets(status="open", db_path=_WAREHOUSE_PATH)
        self.assertIsInstance(rows, list)

    def test_open_bets_have_expected_keys(self):
        from weatherlab.cli.queries import get_bets

        rows = get_bets(status="open", db_path=_WAREHOUSE_PATH)
        if rows:
            row = rows[0]
            for key in ("paper_bet_id", "market_ticker", "side", "notional_dollars", "status"):
                self.assertIn(key, row)

    def test_closed_bets_have_outcome_fields(self):
        from weatherlab.cli.queries import get_bets

        rows = get_bets(status="settled", db_path=_WAREHOUSE_PATH)
        if rows:
            row = rows[0]
            for key in ("outcome_label", "realized_pnl", "closed_at_utc"):
                self.assertIn(key, row)

    def test_all_status_returns_all(self):
        from weatherlab.cli.queries import get_bets

        all_rows = get_bets(status="all", limit=1000, db_path=_WAREHOUSE_PATH)
        open_rows = get_bets(status="open", limit=1000, db_path=_WAREHOUSE_PATH)
        closed_rows = get_bets(status="settled", limit=1000, db_path=_WAREHOUSE_PATH)
        # all >= open + closed (may have other statuses)
        self.assertGreaterEqual(len(all_rows), len(open_rows))

    def test_limit_is_respected(self):
        from weatherlab.cli.queries import get_bets

        rows = get_bets(status="all", limit=3, db_path=_WAREHOUSE_PATH)
        self.assertLessEqual(len(rows), 3)

    def test_no_crash_on_empty_db(self):
        from weatherlab.cli.queries import get_bets

        rows = get_bets(status="open", db_path=":memory:")
        self.assertEqual(rows, [])


class TestGetPnlSummary(unittest.TestCase):
    def test_returns_dict_with_expected_keys(self):
        from weatherlab.cli.queries import get_pnl_summary

        result = get_pnl_summary(db_path=_WAREHOUSE_PATH)
        self.assertIsInstance(result, dict)
        for key in ("total_settled", "wins", "losses", "realized_pnl", "open_count"):
            self.assertIn(key, result)

    def test_counts_are_non_negative(self):
        from weatherlab.cli.queries import get_pnl_summary

        result = get_pnl_summary(db_path=_WAREHOUSE_PATH)
        self.assertGreaterEqual(result["total_settled"], 0)
        self.assertGreaterEqual(result["wins"], 0)
        self.assertGreaterEqual(result["losses"], 0)
        self.assertGreaterEqual(result["open_count"], 0)

    def test_no_crash_on_empty_db(self):
        from weatherlab.cli.queries import get_pnl_summary

        result = get_pnl_summary(db_path=":memory:")
        self.assertIsInstance(result, dict)
        self.assertEqual(result["total_settled"], 0)


class TestGetCalibrationByCity(unittest.TestCase):
    def test_returns_list(self):
        from weatherlab.cli.queries import get_calibration_by_city

        rows = get_calibration_by_city(days=90, db_path=_WAREHOUSE_PATH)
        self.assertIsInstance(rows, list)

    def test_row_has_expected_keys(self):
        from weatherlab.cli.queries import get_calibration_by_city

        rows = get_calibration_by_city(days=90, db_path=_WAREHOUSE_PATH)
        if rows:
            row = rows[0]
            for key in ("city_id", "bet_count", "win_rate", "avg_edge", "avg_pnl"):
                self.assertIn(key, row)

    def test_win_rate_between_0_and_1(self):
        from weatherlab.cli.queries import get_calibration_by_city

        rows = get_calibration_by_city(days=90, db_path=_WAREHOUSE_PATH)
        for row in rows:
            wr = row.get("win_rate")
            if wr is not None:
                self.assertGreaterEqual(float(wr), 0.0)
                self.assertLessEqual(float(wr), 1.0)

    def test_city_filter_works(self):
        from weatherlab.cli.queries import get_calibration_by_city

        rows = get_calibration_by_city(days=90, city="nyc", db_path=_WAREHOUSE_PATH)
        for row in rows:
            self.assertEqual(row.get("city_id", "").lower(), "nyc")

    def test_no_crash_on_empty_db(self):
        from weatherlab.cli.queries import get_calibration_by_city

        rows = get_calibration_by_city(days=90, db_path=":memory:")
        self.assertEqual(rows, [])


class TestGetCalibrationByEdgeBand(unittest.TestCase):
    def test_returns_list(self):
        from weatherlab.cli.queries import get_calibration_by_edge_band

        rows = get_calibration_by_edge_band(days=90, db_path=_WAREHOUSE_PATH)
        self.assertIsInstance(rows, list)

    def test_row_has_expected_keys(self):
        from weatherlab.cli.queries import get_calibration_by_edge_band

        rows = get_calibration_by_edge_band(days=90, db_path=_WAREHOUSE_PATH)
        if rows:
            row = rows[0]
            for key in ("edge_band", "bet_count", "win_rate", "expected_win_rate", "total_pnl"):
                self.assertIn(key, row)

    def test_no_crash_on_empty_db(self):
        from weatherlab.cli.queries import get_calibration_by_edge_band

        rows = get_calibration_by_edge_band(days=90, db_path=":memory:")
        self.assertEqual(rows, [])


class TestGetSettlementObservation(unittest.TestCase):
    def test_returns_none_for_unknown_station(self):
        from weatherlab.cli.queries import get_settlement_observation

        result = get_settlement_observation(
            station_id="KXXX_FAKE",
            market_date_local=date(2020, 1, 1),
            db_path=_WAREHOUSE_PATH,
        )
        self.assertIsNone(result)

    def test_returns_dict_or_none(self):
        from weatherlab.cli.queries import get_settlement_observation

        result = get_settlement_observation(
            station_id="KNYC",
            market_date_local=date(2026, 3, 23),
            db_path=_WAREHOUSE_PATH,
        )
        if result is not None:
            self.assertIn("observed_high_temp_f", result)
            self.assertIn("source", result)

    def test_no_crash_on_empty_db(self):
        from weatherlab.cli.queries import get_settlement_observation

        result = get_settlement_observation(
            station_id="KNYC",
            market_date_local=date(2026, 3, 23),
            db_path=":memory:",
        )
        self.assertIsNone(result)


class TestSettleOutcomeLogic(unittest.TestCase):
    """Unit tests for outcome computation (no DB needed)."""

    def setUp(self):
        from weatherlab.cli.settle import _compute_outcome
        self._compute = _compute_outcome

    def test_between_inside(self):
        self.assertEqual(self._compute(65.0, "between", 62.5, 67.5), "YES")

    def test_between_at_low_boundary(self):
        self.assertEqual(self._compute(62.5, "between", 62.5, 67.5), "YES")

    def test_between_at_high_boundary_is_no(self):
        # Kalshi convention: strictly less than high
        self.assertEqual(self._compute(67.5, "between", 62.5, 67.5), "NO")

    def test_between_below(self):
        self.assertEqual(self._compute(60.0, "between", 62.5, 67.5), "NO")

    def test_gte_above(self):
        self.assertEqual(self._compute(70.0, ">=", 67.5, None), "YES")

    def test_gte_at_threshold(self):
        self.assertEqual(self._compute(67.5, ">=", 67.5, None), "YES")

    def test_gte_below(self):
        self.assertEqual(self._compute(60.0, ">=", 67.5, None), "NO")

    def test_lte_below_is_yes(self):
        # Kalshi "below X" means strictly below
        self.assertEqual(self._compute(60.0, "<=", 67.5, None), "YES")

    def test_lte_at_threshold_is_no(self):
        self.assertEqual(self._compute(67.5, "<=", 67.5, None), "NO")

    def test_unknown_operator_returns_none(self):
        self.assertIsNone(self._compute(65.0, "unknown_op", 62.5, None))

    def test_none_operator_returns_none(self):
        self.assertIsNone(self._compute(65.0, None, 62.5, None))


class TestFormatters(unittest.TestCase):
    """Smoke tests for formatter functions — just verify they return strings."""

    def test_format_status_smoke(self):
        from weatherlab.cli.formatters import format_status

        out = format_status(
            sync_status={"last_kalshi_ts": None, "last_forecast_ts": None},
            board_rows=[],
            open_bets=[],
            pnl={"total_settled": 0, "wins": 0, "losses": 0, "realized_pnl": None, "open_count": 0},
            kill_switch=True,
            today="2026-04-14",
        )
        self.assertIn("CHIEF STATUS", out)
        self.assertIn("SYNC", out)
        self.assertIn("BOARD", out)
        self.assertIn("P&L", out)

    def test_format_board_empty(self):
        from weatherlab.cli.formatters import format_board

        out = format_board([], "2026-04-14")
        self.assertIn("No board data", out)

    def test_format_bets_empty(self):
        from weatherlab.cli.formatters import format_bets

        out = format_bets([], "open")
        self.assertIn("no open bets", out)

    def test_format_calibration_no_data(self):
        from weatherlab.cli.formatters import format_calibration

        out = format_calibration([], [], 90)
        self.assertIn("CALIBRATION", out)

    def test_format_killswitch_on_to_off(self):
        from weatherlab.cli.formatters import format_killswitch

        out = format_killswitch(True, False)
        self.assertIn("ON -> OFF", out)
        self.assertIn("ENABLED", out)

    def test_format_killswitch_off_to_on(self):
        from weatherlab.cli.formatters import format_killswitch

        out = format_killswitch(False, True)
        self.assertIn("OFF -> ON", out)
        self.assertIn("DISABLED", out)


if __name__ == "__main__":
    unittest.main()
