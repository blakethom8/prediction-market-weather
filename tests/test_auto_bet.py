import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import Mock, patch

from weatherlab.build.bootstrap import bootstrap
from weatherlab.db import connect
from weatherlab.pipeline.auto_bet import (
    AUTO_BET_CONFIG,
    compute_coldmath_bet_size,
    compute_bet_size,
    evaluate_auto_bet_candidates,
    format_no_auto_bet_notification,
    get_daily_spend,
    run_auto_betting_session,
    should_auto_bet,
)


class AutoBetTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / 'auto_bet.duckdb'
        bootstrap(db_path=self.db_path)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_compute_bet_size_respects_per_bet_cap(self):
        contracts, cost = compute_bet_size(0.39, 10.0)
        self.assertEqual(contracts, 12)
        self.assertAlmostEqual(cost, 4.68)

    def test_compute_coldmath_bet_size_targets_small_deployments(self):
        contracts, cost = compute_coldmath_bet_size(0.90, 5.0)
        self.assertEqual(contracts, 3)
        self.assertAlmostEqual(cost, 2.70)

    def test_should_auto_bet_blocks_skip_city(self):
        candidate = {
            'city_key': 'hou',
            'city_name': 'Houston',
            'best_bucket': 'KXHIGHTHOU-26MAR24-T83',
            'best_bucket_ask': 0.06,
            'edge': 0.30,
            'forecast_confidence': 'high',
            'obs_count': 6,
            'obs_forecast_divergence_f': 1.0,
            'scan_date': '2026-03-24',
            '_db_path': self.db_path,
        }

        with patch('weatherlab.pipeline.auto_bet._kill_switch_path', return_value=Path(self.tmpdir.name) / 'nonexistent'):
            should_bet, reason = should_auto_bet(candidate)
        self.assertFalse(should_bet)
        self.assertEqual(reason, 'station unverified')

    def test_run_auto_betting_session_splits_edge_and_coldmath_budgets(self):
        scan_results = {
            'scan_date': '2026-03-24',
            'scan_time_utc': '2026-03-24T17:00:00Z',
            'cities': {
                'miami': {
                    'city_key': 'miami',
                    'city_name': 'Miami',
                    'station_id': 'KMIA',
                    'station_verified': True,
                    'forecast_high_f': 83,
                    'forecast_confidence': 'high',
                    'observed_max_so_far_f': 79.0,
                    'obs_count': 6,
                    'obs_forecast_divergence_f': 4.0,
                    'validation_note': 'Tracking well.',
                    'best_bucket': 'KXHIGHMIA-26MAR24-B82.5',
                    'best_bucket_code': 'B82.5',
                    'best_bucket_label': '82° to 83°F',
                    'best_bucket_center_f': 82.5,
                    'best_bucket_ask': 0.25,
                    'model_probability': 0.65,
                    'edge': 0.40,
                    'market_favorite_bucket': 'KXHIGHMIA-26MAR24-B82.5',
                    'market_favorite_bucket_code': 'B82.5',
                    'market_favorite_label': '82° to 83°F',
                    'market_favorite_center_f': 82.5,
                    'market_favorite_ask': 0.25,
                    'adjacent_bucket': None,
                    'adjacent_bucket_label': None,
                    'recommendation': 'BUY',
                    'recommendation_reason': 'Positive edge with high-confidence station validation.',
                },
                'dc': {
                    'city_key': 'dc',
                    'city_name': 'DC',
                    'station_id': 'KDCA',
                    'station_verified': True,
                    'forecast_high_f': 51,
                    'forecast_confidence': 'high',
                    'observed_max_so_far_f': 48.0,
                    'obs_count': 5,
                    'obs_forecast_divergence_f': 3.0,
                    'validation_note': 'Tracking well.',
                    'best_bucket': 'KXHIGHTDC-26MAR24-T52',
                    'best_bucket_code': 'T52',
                    'best_bucket_label': '<52°F',
                    'best_bucket_center_f': 52.0,
                    'best_bucket_ask': 0.10,
                    'model_probability': 0.35,
                    'edge': 0.25,
                    'market_favorite_bucket': 'KXHIGHTDC-26MAR24-T52',
                    'market_favorite_bucket_code': 'T52',
                    'market_favorite_label': '<52°F',
                    'market_favorite_center_f': 52.0,
                    'market_favorite_ask': 0.10,
                    'adjacent_bucket': None,
                    'adjacent_bucket_label': None,
                    'recommendation': 'BUY',
                    'recommendation_reason': 'Positive edge with high-confidence station validation.',
                },
            },
            'coldmath_plays': [
                {
                    'city': 'Los Angeles',
                    'city_key': 'lax',
                    'station_id': 'KLAX',
                    'ticker': 'KXHIGHLAX-26MAR24-T76',
                    'contract_type': 'threshold',
                    'label': 'below 76°F',
                    'bet_side': 'YES',
                    'bet_price': 0.90,
                    'yes_ask': 0.90,
                    'no_equivalent': 0.10,
                    'forecast_f': 66.0,
                    'threshold_f': 76.0,
                    'forecast_gap_f': 10.0,
                    'confidence': 'high',
                    'win_per_contract': 0.10,
                    'recommendation': 'BUY',
                    'score': 9.0,
                    'thesis': 'NWS=66°F, threshold=76°F, 10°F margin - near-certain YES',
                },
            ],
            'top_picks': ['miami', 'dc'],
        }
        fake_client = Mock()
        fake_client.place_order.side_effect = [
            {'order': {'order_id': 'order-miami', 'status': 'executed', 'count': 20, 'fill_count': 20, 'limit_price': 25, 'taker_cost_dollars': 5.0}},
            {'order': {'order_id': 'order-dc', 'status': 'executed', 'count': 50, 'fill_count': 50, 'limit_price': 10, 'taker_cost_dollars': 5.0}},
            {'order': {'order_id': 'order-lax', 'status': 'executed', 'count': 3, 'fill_count': 3, 'limit_price': 90, 'taker_cost_dollars': 2.7}},
        ]

        nonexistent = Path(self.tmpdir.name) / 'nonexistent'
        with patch('weatherlab.pipeline.auto_bet.KalshiClient', return_value=fake_client), \
             patch('weatherlab.pipeline.auto_bet.time_module.time', side_effect=[1774339200, 1774339201, 1774339202]), \
             patch('weatherlab.pipeline.auto_bet._kill_switch_path', return_value=nonexistent), \
             patch('weatherlab.pipeline.auto_bet.is_paper_mode', return_value=False):
                placed = run_auto_betting_session(scan_results, db_path=self.db_path)

        placed_strategies = [bet['bet_strategy'] for bet in placed]
        self.assertIn('edge', placed_strategies)
        self.assertIn('coldmath', placed_strategies)
        total_spend = get_daily_spend(date(2026, 3, 24), db_path=self.db_path)
        self.assertGreater(total_spend, 5.0)
        self.assertAlmostEqual(get_daily_spend(date(2026, 3, 24), db_path=self.db_path, bet_strategy='edge'), 10.0)
        self.assertAlmostEqual(get_daily_spend(date(2026, 3, 24), db_path=self.db_path, bet_strategy='coldmath'), 2.7)
        con = connect(read_only=True, db_path=self.db_path)
        try:
            live_order_count = con.execute('select count(*) from ops.live_orders').fetchone()[0]
            calibration_rows = con.execute(
                'select bet_strategy from ops.calibration_log order by bet_strategy'
            ).fetchall()
        finally:
            con.close()
        # With $10 edge budget, both miami and dc edge bets fire + 1 coldmath
        self.assertGreaterEqual(live_order_count, 2)
        strategies = sorted([row[0] for row in calibration_rows])
        self.assertIn('coldmath', strategies)
        self.assertIn('edge', strategies)

    def test_format_no_auto_bet_notification_surfaces_candidate_reasons(self):
        scan_results = {
            'scan_date': '2026-03-24',
            'scan_time_utc': '2026-03-24T15:00:00Z',
            'cities': {
                'miami': {
                    'city_key': 'miami',
                    'city_name': 'Miami',
                    'station_id': 'KMIA',
                    'station_verified': True,
                    'forecast_high_f': 83,
                    'forecast_confidence': 'high',
                    'observed_max_so_far_f': 79.0,
                    'obs_count': 6,
                    'obs_forecast_divergence_f': 4.0,
                    'best_bucket': 'KXHIGHMIA-26MAR24-B82.5',
                    'best_bucket_code': 'B82.5',
                    'best_bucket_label': '82° to 83°F',
                    'best_bucket_ask': 0.49,
                    'edge': 0.18,
                    'recommendation': 'WATCH',
                    'recommendation_reason': 'Edge is too thin after confidence adjustment.',
                },
                'phl': {
                    'city_key': 'phl',
                    'city_name': 'Philly',
                    'station_id': 'KPHL',
                    'station_verified': True,
                    'forecast_high_f': 50,
                    'forecast_confidence': 'medium',
                    'observed_max_so_far_f': 42.0,
                    'obs_count': 4,
                    'obs_forecast_divergence_f': 8.0,
                    'best_bucket': 'KXHIGHPHIL-26MAR24-B50.5',
                    'best_bucket_code': 'B50.5',
                    'best_bucket_label': '50° to 51°F',
                    'best_bucket_ask': 0.22,
                    'edge': 0.30,
                    'recommendation': 'WATCH',
                    'recommendation_reason': 'Observed trend diverges from forecast.',
                },
                'hou': {
                    'city_key': 'hou',
                    'city_name': 'Houston',
                    'station_id': 'KHOU',
                    'station_verified': False,
                    'forecast_high_f': 84,
                    'forecast_confidence': 'high',
                    'observed_max_so_far_f': 80.0,
                    'obs_count': 5,
                    'obs_forecast_divergence_f': 4.0,
                    'best_bucket': 'KXHIGHTHOU-26MAR24-T83',
                    'best_bucket_code': 'T83',
                    'best_bucket_label': '<83°F',
                    'best_bucket_ask': 0.06,
                    'edge': 0.25,
                    'recommendation': 'SKIP',
                    'recommendation_reason': 'Settlement station is not verified against current contract rules.',
                },
            },
        }

        evaluations = evaluate_auto_bet_candidates(scan_results, db_path=self.db_path)
        report = format_no_auto_bet_notification(scan_results, evaluations)

        self.assertIn('[EDGE] Miami B82.5 — edge 18¢ (below 20¢ threshold)', report)
        self.assertIn('[EDGE] Philly B50.5 — medium confidence (ASOS diverging)', report)
        self.assertIn('[EDGE] Houston T83 — skipped (station unverified)', report)


if __name__ == '__main__':
    unittest.main()
