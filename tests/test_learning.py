import json
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

from weatherlab.build.bootstrap import bootstrap
from weatherlab.db import connect
from weatherlab.live.live_orders import fetch_live_orders, seed_live_order
from weatherlab.pipeline.learning import (
    append_insights_to_file,
    format_settlement_notification,
    record_bet_outcome,
    run_settlement_and_learning,
    write_daily_memory,
)


class LearningPipelineTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / 'learning.duckdb'
        bootstrap(db_path=self.db_path)

    def tearDown(self):
        self.tmpdir.cleanup()

    def _insert_pending_calibration(
        self,
        *,
        live_order_id: str,
        market_date_local: date,
        city_key: str,
        station_id: str,
        ticker: str,
        our_forecast_f: float,
        forecast_confidence: str,
        market_ask_price: float,
        bucket_center_f: float,
        notes: dict,
    ) -> None:
        con = connect(db_path=self.db_path)
        try:
            con.execute(
                '''
                insert into ops.calibration_log (
                    log_id,
                    market_date_local,
                    city_key,
                    station_id,
                    ticker,
                    live_order_id,
                    is_paper_bet,
                    our_forecast_f,
                    forecast_confidence,
                    market_ask_price,
                    bucket_center_f,
                    notes
                ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                [
                    live_order_id,
                    market_date_local,
                    city_key,
                    station_id,
                    ticker,
                    live_order_id,
                    False,
                    our_forecast_f,
                    forecast_confidence,
                    market_ask_price,
                    bucket_center_f,
                    json.dumps(notes, sort_keys=True),
                ],
            )
        finally:
            con.close()

    def test_record_bet_outcome_settles_live_order_and_updates_calibration_log(self):
        live_order_id = seed_live_order(
            kalshi_order_id='order-dc-1',
            ticker='KXHIGHTDC-26MAR23-T67',
            action='buy',
            side='yes',
            order_type='limit',
            limit_price_cents=1,
            initial_count=200,
            fill_count=200,
            remaining_count=0,
            status='executed',
            taker_cost_dollars=2.0,
            db_path=self.db_path,
        )
        self._insert_pending_calibration(
            live_order_id=live_order_id,
            market_date_local=date(2026, 3, 23),
            city_key='dc',
            station_id='KDCA',
            ticker='KXHIGHTDC-26MAR23-T67',
            our_forecast_f=52.0,
            forecast_confidence='high',
            market_ask_price=0.01,
            bucket_center_f=67.0,
            notes={
                'market_favorite_ticker': 'KXHIGHTDC-26MAR23-T67',
                'market_favorite_center_f': 67.0,
            },
        )

        summary = record_bet_outcome(
            live_order_id,
            actual_high_f=67.8,
            station_id='KDCA',
            our_forecast_f=52.0,
            forecast_confidence='high',
            db_path=self.db_path,
        )

        self.assertEqual(summary['outcome'], 'no')
        self.assertAlmostEqual(summary['realized_pnl_dollars'], -2.0)
        row = fetch_live_orders(db_path=self.db_path, live_order_id=live_order_id)[0]
        self.assertEqual(row['status'], 'settled')

        con = connect(read_only=True, db_path=self.db_path)
        try:
            calibration = con.execute(
                '''
                select actual_high_f, outcome, forecast_error_f, edge_realized
                from ops.calibration_log
                where live_order_id = ?
                ''',
                [live_order_id],
            ).fetchone()
            summary_row = con.execute(
                '''
                select total_bets, total_pnl
                from ops.v_calibration_summary
                where forecast_confidence = 'high' and city_key = 'dc'
                '''
            ).fetchone()
        finally:
            con.close()

        self.assertEqual(calibration[0], 67.8)
        self.assertEqual(calibration[1], 'no')
        self.assertAlmostEqual(calibration[2], 15.8)
        self.assertAlmostEqual(calibration[3], -2.0)
        self.assertEqual(summary_row[0], 1)
        self.assertAlmostEqual(summary_row[1], -2.0)

    def test_run_settlement_and_learning_writes_insights_and_memory_outputs(self):
        live_order_id = seed_live_order(
            kalshi_order_id='order-lax-1',
            ticker='KXHIGHLAX-26MAR23-T76',
            action='buy',
            side='yes',
            order_type='limit',
            limit_price_cents=92,
            initial_count=5,
            fill_count=5,
            remaining_count=0,
            status='executed',
            taker_cost_dollars=4.60,
            db_path=self.db_path,
        )
        self._insert_pending_calibration(
            live_order_id=live_order_id,
            market_date_local=date(2026, 3, 23),
            city_key='lax',
            station_id='KLAX',
            ticker='KXHIGHLAX-26MAR23-T76',
            our_forecast_f=70.0,
            forecast_confidence='high',
            market_ask_price=0.92,
            bucket_center_f=76.0,
            notes={
                'market_favorite_ticker': 'KXHIGHLAX-26MAR23-T76',
                'market_favorite_center_f': 76.0,
            },
        )

        with patch(
            'weatherlab.settlement.kalshi_settlement.fetch_market_result',
            return_value={
                'status': 'finalized',
                'result': 'yes',
                'title': 'Will the high temp at LAX stay below 76F?',
            },
        ), patch(
            'weatherlab.settlement.kalshi_settlement.fetch_actual_high_from_kalshi',
            return_value=70.5,
        ), patch('weatherlab.pipeline.learning.fetch_station_daily_high', return_value=71.2):
            report = run_settlement_and_learning(date(2026, 3, 23), db_path=self.db_path)

        self.assertAlmostEqual(report['session_pnl'], 0.40)
        self.assertAlmostEqual(report['cumulative_pnl'], 0.40)
        self.assertIn('our model beat the market favorite', report['insights_text'])
        notification = format_settlement_notification(report, insights_updated=True)
        self.assertIn('KLAX (Los Angeles): official ~70.5°F | ASOS 71.2°F', notification)
        self.assertIn('Kalshi-confirmed YES', notification)
        self.assertIn('Session P&L: +$0.40', notification)
        self.assertIn('📝 BETTING_INSIGHTS.md updated', notification)

        insights_path = Path(self.tmpdir.name) / 'BETTING_INSIGHTS.md'
        append_insights_to_file(report['insights_text'], insights_path=str(insights_path))
        self.assertIn('Settlement Review', insights_path.read_text())

        memory_dir = Path(self.tmpdir.name) / 'memory'
        write_daily_memory(report, memory_dir=str(memory_dir))
        memory_path = memory_dir / '2026-03-23.md'
        self.assertTrue(memory_path.exists())
        self.assertIn('Official high (Kalshi/NWS)', memory_path.read_text())


if __name__ == '__main__':
    unittest.main()
