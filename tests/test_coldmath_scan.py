import unittest
from datetime import date
from unittest.mock import Mock, patch

from weatherlab.pipeline.morning_scan import format_scan_report, scan_coldmath_plays


def _default_validation() -> dict:
    return {
        'forecast_high_f': None,
        'observed_max_so_far_f': None,
        'obs_count': 0,
        'forecast_confidence': 'unknown',
        'note': 'No observations yet.',
    }


class ColdMathScanTests(unittest.TestCase):
    def _run_scan(self, *, markets: list[dict], validations: dict[str, dict], min_forecast_gap_f: float = 8.0) -> list[dict]:
        fake_client = Mock()
        fake_client.fetch_open_weather_markets.return_value = markets

        def fake_validation(station_id: str) -> dict:
            return validations.get(station_id, _default_validation())

        with patch('weatherlab.pipeline.morning_scan.KalshiClient', return_value=fake_client):
            with patch('weatherlab.pipeline.morning_scan.fetch_morning_validation', side_effect=fake_validation):
                return scan_coldmath_plays(
                    target_date=date(2026, 3, 24),
                    min_forecast_gap_f=min_forecast_gap_f,
                )

    def test_scan_coldmath_plays_identifies_threshold_and_bucket_candidates(self):
        plays = self._run_scan(
            markets=[
                {
                    'ticker': 'KXHIGHLAX-26MAR24-T76',
                    'title': 'Will the high temp in Los Angeles be below 76 degrees on Mar 24, 2026?',
                    'yes_ask': 0.90,
                },
                {
                    'ticker': 'KXHIGHMIA-26MAR24-B82.5',
                    'title': 'Will the high temp in Miami be between 82 and 83 degrees on Mar 24, 2026?',
                    'yes_ask': 0.02,
                },
            ],
            validations={
                'KLAX': {
                    'forecast_high_f': 68,
                    'observed_max_so_far_f': 61.0,
                    'obs_count': 5,
                    'forecast_confidence': 'high',
                    'note': 'Tracking well.',
                },
                'KMIA': {
                    'forecast_high_f': 66,
                    'observed_max_so_far_f': 77.0,
                    'obs_count': 6,
                    'forecast_confidence': 'high',
                    'note': 'Tracking well.',
                },
            },
        )

        self.assertEqual([play['ticker'] for play in plays], ['KXHIGHMIA-26MAR24-B82.5', 'KXHIGHLAX-26MAR24-T76'])

        bucket_play = plays[0]
        self.assertEqual(bucket_play['contract_type'], 'bucket')
        self.assertEqual(bucket_play['bet_side'], 'NO')
        self.assertEqual(bucket_play['bet_price'], 0.98)
        self.assertEqual(bucket_play['threshold_f'], 82.0)
        self.assertEqual(bucket_play['forecast_gap_f'], 16.0)

        threshold_play = plays[1]
        self.assertEqual(threshold_play['contract_type'], 'threshold')
        self.assertEqual(threshold_play['label'], 'below 76°F')
        self.assertEqual(threshold_play['bet_side'], 'YES')
        self.assertEqual(threshold_play['yes_ask'], 0.90)
        self.assertEqual(threshold_play['forecast_gap_f'], 8.0)

    def test_scan_coldmath_confidence_scoring(self):
        plays = self._run_scan(
            markets=[
                {
                    'ticker': 'KXHIGHDEN-26MAR24-T70',
                    'title': 'Will the high temp in Denver be below 70 degrees on Mar 24, 2026?',
                    'yes_ask': 0.91,
                },
                {
                    'ticker': 'KXHIGHLAX-26MAR24-T76',
                    'title': 'Will the high temp in Los Angeles be below 76 degrees on Mar 24, 2026?',
                    'yes_ask': 0.90,
                },
                {
                    'ticker': 'KXHIGHSEA-26MAR24-T85',
                    'title': 'Will the high temp in Seattle be below 85 degrees on Mar 24, 2026?',
                    'yes_ask': 0.89,
                },
            ],
            validations={
                'KDEN': {'forecast_high_f': 57, 'observed_max_so_far_f': 50.0, 'obs_count': 5, 'forecast_confidence': 'high', 'note': 'Tracking well.'},
                'KLAX': {'forecast_high_f': 68, 'observed_max_so_far_f': 61.0, 'obs_count': 5, 'forecast_confidence': 'high', 'note': 'Tracking well.'},
                'KSEA': {'forecast_high_f': 79, 'observed_max_so_far_f': 70.0, 'obs_count': 5, 'forecast_confidence': 'high', 'note': 'Tracking well.'},
            },
            min_forecast_gap_f=5.0,
        )

        plays_by_ticker = {play['ticker']: play for play in plays}
        self.assertEqual(plays_by_ticker['KXHIGHDEN-26MAR24-T70']['confidence'], 'very_high')
        self.assertEqual(plays_by_ticker['KXHIGHLAX-26MAR24-T76']['confidence'], 'high')
        self.assertEqual(plays_by_ticker['KXHIGHSEA-26MAR24-T85']['confidence'], 'medium')

    def test_scan_coldmath_empty_when_no_gap_meets_threshold(self):
        plays = self._run_scan(
            markets=[
                {
                    'ticker': 'KXHIGHLAX-26MAR24-T76',
                    'title': 'Will the high temp in Los Angeles be below 76 degrees on Mar 24, 2026?',
                    'yes_ask': 0.90,
                },
            ],
            validations={
                'KLAX': {'forecast_high_f': 72, 'observed_max_so_far_f': 66.0, 'obs_count': 5, 'forecast_confidence': 'high', 'note': 'Tracking well.'},
            },
        )

        self.assertEqual(plays, [])

    def test_scan_coldmath_respects_eight_degree_gap_filter(self):
        plays = self._run_scan(
            markets=[
                {
                    'ticker': 'KXHIGHLAX-26MAR24-T76',
                    'title': 'Will the high temp in Los Angeles be below 76 degrees on Mar 24, 2026?',
                    'yes_ask': 0.90,
                },
                {
                    'ticker': 'KXHIGHTDC-26MAR24-T52',
                    'title': 'Will the high temp in Washington, DC be below 52 degrees on Mar 24, 2026?',
                    'yes_ask': 0.90,
                },
            ],
            validations={
                'KLAX': {'forecast_high_f': 68, 'observed_max_so_far_f': 61.0, 'obs_count': 5, 'forecast_confidence': 'high', 'note': 'Tracking well.'},
                'KDCA': {'forecast_high_f': 45, 'observed_max_so_far_f': 39.0, 'obs_count': 5, 'forecast_confidence': 'high', 'note': 'Tracking well.'},
            },
        )

        self.assertEqual([play['ticker'] for play in plays], ['KXHIGHLAX-26MAR24-T76'])

    def test_format_scan_report_includes_coldmath_section_when_plays_exist(self):
        report = format_scan_report(
            {
                'scan_date': '2026-03-24',
                'scan_time_utc': '2026-03-24T15:00:00Z',
                'cities': {},
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
                        'forecast_f': 68.0,
                        'threshold_f': 76.0,
                        'forecast_gap_f': 8.0,
                        'confidence': 'high',
                        'win_per_contract': 0.10,
                        'recommendation': 'WATCH',
                        'score': 7.2,
                        'thesis': 'NWS=68°F, threshold=76°F, 8°F margin - near-certain YES',
                    },
                ],
            }
        )

        self.assertIn('🎯 COLDMATH LAYER - Near-Certain Plays', report)
        self.assertIn('Los Angeles below 76°F - YES @ 90¢ (+10¢/contract)', report)
        self.assertIn('Auto-bet fires at 10 AM PDT.', report)


if __name__ == '__main__':
    unittest.main()
