import tempfile
import unittest
from pathlib import Path

from weatherlab.backtest.decision_logger import log_decision
from weatherlab.backtest.rationale import build_rationale
from weatherlab.build.bootstrap import bootstrap
from weatherlab.db import connect


class BootstrapAndLoggingTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / "test_weather.duckdb"
        bootstrap(db_path=self.db_path)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_bootstrap_creates_expected_tables(self):
        con = connect(db_path=self.db_path)
        try:
            tables = {
                row[0]
                for row in con.execute(
                    "select table_schema || '.' || table_name from information_schema.tables"
                ).fetchall()
            }
            board_columns = {
                row[1]
                for row in con.execute("pragma table_info('ops.strategy_market_board')").fetchall()
            }
        finally:
            con.close()

        self.assertIn("core.weather_contracts", tables)
        self.assertIn("core.forecast_snapshots", tables)
        self.assertIn("ops.decision_journal", tables)
        self.assertIn("ops.strategy_sessions", tables)
        self.assertIn("ops.strategy_review_events", tables)
        self.assertIn("ops.bet_proposals", tables)
        self.assertIn("ops.bet_proposal_events", tables)
        self.assertIn("ops.paper_bets", tables)
        self.assertIn("ops.paper_bet_reviews", tables)
        self.assertNotIn("settlement_source", board_columns)

    def test_decision_logger_writes_row(self):
        rationale = build_rationale(
            fair_prob=0.61,
            market_mid=0.48,
            tradable_yes_ask=0.5,
            edge_vs_ask=0.11,
            minutes_to_close=180,
        )
        decision_id = log_decision(
            market_ticker="TEST_MARKET",
            signal_version="baseline-v1",
            fair_prob=0.61,
            market_mid=0.48,
            tradable_yes_ask=0.5,
            tradable_yes_bid=0.46,
            edge_vs_mid=0.13,
            edge_vs_ask=0.11,
            confidence=0.72,
            action="BUY_YES",
            abstain_reason=None,
            rationale=rationale,
            db_path=self.db_path,
        )
        con = connect(db_path=self.db_path)
        try:
            row = con.execute(
                "select market_ticker, action, fair_prob from ops.decision_journal where decision_id = ?",
                [decision_id],
            ).fetchone()
        finally:
            con.close()

        self.assertEqual(row[0], "TEST_MARKET")
        self.assertEqual(row[1], "BUY_YES")
        self.assertAlmostEqual(row[2], 0.61)


if __name__ == "__main__":
    unittest.main()
