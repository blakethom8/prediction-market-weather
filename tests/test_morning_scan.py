import unittest
from datetime import date
from unittest.mock import Mock, patch

from weatherlab.pipeline._markets import choose_best_market, estimate_model_probability, parse_weather_market
from weatherlab.pipeline.morning_scan import _recommendation_for_city, format_scan_report, run_morning_scan


class MorningScanTests(unittest.TestCase):
    def test_recommendation_buys_when_observed_max_reaches_threshold_by_10am(self):
        market = parse_weather_market(
            {
                'ticker': 'KXHIGHMIA-26APR26-A80',
                'title': 'Will the high temp in Miami be above 80 degrees on Apr 26, 2026?',
                'yes_ask': 0.42,
            }
        )
        self.assertIsNotNone(market)
        assert market is not None

        recommendation, reason = _recommendation_for_city(
            station_verified=True,
            confidence='medium',
            best_market=market,
            model_probability=0.20,
            adjacent_market=None,
            observed_max_so_far_f=80.0,
            forecast_high_f=77.0,
            obs_divergence_f=3.0,
            local_hour=10,
        )

        self.assertEqual(recommendation, 'BUY')
        self.assertIn('Observed max already reached', reason)

    def test_recommendation_downgrades_when_observed_max_lags_forecast_after_11am(self):
        market = parse_weather_market(
            {
                'ticker': 'KXHIGHMIA-26APR26-B82.5',
                'title': 'Will the high temp in Miami be between 82 and 83 degrees on Apr 26, 2026?',
                'yes_ask': 0.39,
            }
        )
        self.assertIsNotNone(market)
        assert market is not None

        recommendation, reason = _recommendation_for_city(
            station_verified=True,
            confidence='high',
            best_market=market,
            model_probability=0.65,
            adjacent_market=None,
            observed_max_so_far_f=79.0,
            forecast_high_f=83.0,
            obs_divergence_f=4.0,
            local_hour=11,
        )

        self.assertEqual(recommendation, 'WATCH')
        self.assertIn('tracking 4.0F below forecast', reason)

    def test_run_morning_scan_scores_edges_and_recommendations(self):
        target_date = date(2026, 3, 24)
        fake_client = Mock()
        fake_client.fetch_open_weather_markets.return_value = [
            {
                'ticker': 'KXHIGHMIA-26MAR24-B82.5',
                'title': 'Will the high temp in Miami be between 82 and 83 degrees on Mar 24, 2026?',
                'yes_ask': 0.39,
            },
            {
                'ticker': 'KXHIGHPHIL-26MAR24-B50.5',
                'title': 'Will the high temp in Philadelphia be between 50 and 51 degrees on Mar 24, 2026?',
                'yes_ask': 0.33,
            },
            {
                'ticker': 'KXHIGHTBOS-26MAR24-T36',
                'title': 'Will the high temp in Boston be below 36 degrees on Mar 24, 2026?',
                'yes_ask': 0.18,
            },
            {
                'ticker': 'KXHIGHMIA-26MAR25-B82.5',
                'title': 'Will the high temp in Miami be between 82 and 83 degrees on Mar 25, 2026?',
                'yes_ask': 0.41,
            },
        ]

        validations = {
            'KMIA': {
                'forecast_high_f': 83,
                'observed_max_so_far_f': 76.2,
                'obs_count': 7,
                'forecast_confidence': 'high',
                'note': 'Observed temps are tracking within 2.0F of forecast.',
            },
            'KPHL': {
                'forecast_high_f': 51,
                'observed_max_so_far_f': 44.0,
                'obs_count': 4,
                'forecast_confidence': 'medium',
                'note': 'Observed max is 7.0F away from forecast with 4 observations so far.',
            },
            'KBOS': {
                'forecast_high_f': 36,
                'observed_max_so_far_f': 43.0,
                'obs_count': 4,
                'forecast_confidence': 'low',
                'note': 'Observed max already exceeds the NWS high by 7.0F.',
            },
        }

        def fake_validation(station_id: str) -> dict:
            return validations.get(
                station_id,
                {
                    'forecast_high_f': None,
                    'observed_max_so_far_f': None,
                    'obs_count': 0,
                    'forecast_confidence': 'unknown',
                    'note': 'No observations yet.',
                },
            )

        with patch('weatherlab.pipeline.morning_scan.KalshiClient', return_value=fake_client):
            with patch('weatherlab.pipeline.morning_scan.fetch_morning_validation', side_effect=fake_validation):
                scan = run_morning_scan(target_date=target_date)

        miami = scan['cities']['miami']
        self.assertEqual(miami['best_bucket'], 'KXHIGHMIA-26MAR24-B82.5')
        self.assertEqual(miami['recommendation'], 'BUY')
        self.assertEqual(miami['model_probability'], 0.65)
        self.assertEqual(miami['edge'], 0.26)

        philly = scan['cities']['phl']
        self.assertEqual(philly['recommendation'], 'WATCH')
        self.assertEqual(philly['best_bucket_label'], '50° to 51°')

        boston = scan['cities']['bos']
        self.assertEqual(boston['recommendation'], 'SKIP')
        self.assertIn('Observed max already exceeds', boston['recommendation_reason'])

        self.assertEqual(scan['top_picks'], ['miami'])

    def test_estimate_model_probability_adjusts_for_confidence(self):
        market = parse_weather_market(
            {
                'ticker': 'KXHIGHMIA-26MAR24-B82.5',
                'title': 'Will the high temp in Miami be between 82 and 83 degrees on Mar 24, 2026?',
                'yes_ask': 0.39,
            }
        )
        self.assertIsNotNone(market)
        assert market is not None

        self.assertEqual(estimate_model_probability(83, 'high', market), 0.65)
        self.assertEqual(estimate_model_probability(83, 'medium', market), 0.55)
        self.assertEqual(estimate_model_probability(84.5, 'high', market), 0.35)

    def test_choose_best_market_skips_low_open_interest_contracts(self):
        low_interest_market = parse_weather_market(
            {
                'ticker': 'KXHIGHMIA-26MAR24-B82.5',
                'title': 'Will the high temp in Miami be between 82 and 83 degrees on Mar 24, 2026?',
                'yes_ask': 0.39,
                'open_interest': 999,
            }
        )
        liquid_market = parse_weather_market(
            {
                'ticker': 'KXHIGHMIA-26MAR24-B84.5',
                'title': 'Will the high temp in Miami be between 84 and 85 degrees on Mar 24, 2026?',
                'yes_ask': 0.21,
                'open_interest': 1000,
            }
        )
        self.assertIsNotNone(low_interest_market)
        self.assertIsNotNone(liquid_market)
        assert low_interest_market is not None
        assert liquid_market is not None

        best_market, model_probability = choose_best_market(
            [low_interest_market, liquid_market],
            forecast_high_f=83,
            confidence='high',
        )

        self.assertEqual(best_market, liquid_market)
        self.assertEqual(model_probability, 0.35)

    def test_run_morning_scan_flags_high_volume_market_disagreement(self):
        target_date = date(2026, 3, 24)
        fake_client = Mock()
        fake_client.fetch_open_weather_markets.return_value = [
            {
                'ticker': 'KXHIGHMIA-26MAR24-B82.5',
                'title': 'Will the high temp in Miami be between 82 and 83 degrees on Mar 24, 2026?',
                'yes_ask': 0.39,
                'volume': 5001,
                'open_interest': 1000,
            },
        ]

        def fake_validation(station_id: str) -> dict:
            if station_id == 'KMIA':
                return {
                    'forecast_high_f': 83,
                    'observed_max_so_far_f': 76.2,
                    'obs_count': 7,
                    'forecast_confidence': 'high',
                    'note': 'Observed temps are tracking within 2.0F of forecast.',
                }
            return {
                'forecast_high_f': None,
                'observed_max_so_far_f': None,
                'obs_count': 0,
                'forecast_confidence': 'unknown',
                'note': 'No observations yet.',
            }

        with patch('weatherlab.pipeline.morning_scan.KalshiClient', return_value=fake_client):
            with patch('weatherlab.pipeline.morning_scan.fetch_morning_validation', side_effect=fake_validation):
                scan = run_morning_scan(target_date=target_date)

        miami = scan['cities']['miami']
        self.assertEqual(miami['recommendation'], 'MARKET_DISAGREES')
        self.assertEqual(
            miami['recommendation_reason'],
            'High-volume market price differs from the model by more than 10¢.',
        )
        self.assertNotIn('miami', scan['top_picks'])

    def test_format_scan_report_renders_sections(self):
        scan_results = {
            'scan_date': '2026-03-24',
            'scan_time_utc': '2026-03-24T15:00:00Z',
            'cities': {
                'miami': {
                    'city_name': 'Miami',
                    'station_id': 'KMIA',
                    'forecast_high_f': 83,
                    'forecast_confidence': 'high',
                    'observed_max_so_far_f': 76.2,
                    'best_bucket': 'KXHIGHMIA-26MAR24-B82.5',
                    'best_bucket_code': 'B82.5',
                    'best_bucket_label': '82° to 83°',
                    'best_bucket_ask': 0.39,
                    'model_probability': 0.65,
                    'edge': 0.26,
                    'adjacent_bucket': None,
                    'adjacent_bucket_label': None,
                    'recommendation': 'BUY',
                    'recommendation_reason': 'Positive edge with high-confidence station validation.',
                },
                'bos': {
                    'city_name': 'Boston',
                    'station_id': 'KBOS',
                    'forecast_high_f': 36,
                    'forecast_confidence': 'low',
                    'observed_max_so_far_f': 43.0,
                    'best_bucket': 'KXHIGHTBOS-26MAR24-T36',
                    'best_bucket_code': 'T36',
                    'best_bucket_label': '<36°F',
                    'best_bucket_ask': 0.18,
                    'model_probability': 0.15,
                    'edge': -0.03,
                    'adjacent_bucket': None,
                    'adjacent_bucket_label': None,
                    'recommendation': 'SKIP',
                    'recommendation_reason': 'Observed max already exceeds the NWS high by 7.0F.',
                },
            },
            'top_picks': ['miami'],
        }

        report = format_scan_report(scan_results, include_all=True)
        self.assertIn('🎯 WEATHER BET SCAN - March 24', report)
        self.assertIn('TOP PICKS:', report)
        self.assertIn('✅ Miami B82.5', report)
        self.assertIn('SKIP:', report)
        self.assertIn('⏭ Boston T36', report)

    def test_run_morning_scan_skips_thin_edge(self):
        target_date = date(2026, 3, 24)
        fake_client = Mock()
        fake_client.fetch_open_weather_markets.return_value = [
            {
                'ticker': 'KXHIGHMIA-26MAR24-B82.5',
                'title': 'Will the high temp in Miami be between 82 and 83 degrees on Mar 24, 2026?',
                'yes_ask': 0.62,
            },
        ]

        with patch('weatherlab.pipeline.morning_scan.KalshiClient', return_value=fake_client):
            with patch(
                'weatherlab.pipeline.morning_scan.fetch_morning_validation',
                return_value={
                    'forecast_high_f': 83,
                    'observed_max_so_far_f': 76.0,
                    'obs_count': 7,
                    'forecast_confidence': 'high',
                    'note': 'Observed temps are tracking within 1.0F of forecast.',
                },
            ):
                scan = run_morning_scan(target_date=target_date)

        self.assertEqual(scan['cities']['miami']['recommendation'], 'SKIP')
        self.assertEqual(scan['cities']['miami']['recommendation_reason'], 'Edge is too thin after confidence adjustment.')


if __name__ == '__main__':
    unittest.main()
