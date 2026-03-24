import unittest
from datetime import UTC, date, datetime
from unittest.mock import patch

from weatherlab.forecast.asos import (
    fetch_morning_validation,
    fetch_station_daily_high,
    fetch_station_observations,
)


class FakeResponse:
    def __init__(self, payload: dict):
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self._payload


class ASOSFetchTests(unittest.TestCase):
    def test_fetch_station_observations_parses_and_sorts_nws_json(self):
        payload = {
            'features': [
                {
                    'properties': {
                        'timestamp': '2026-03-24T11:00:00Z',
                        'temperature': {'value': 18.0},
                        'textDescription': 'Sunny',
                    }
                },
                {
                    'properties': {
                        'timestamp': '2026-03-24T09:00:00Z',
                        'temperature': {'value': 16.0},
                        'textDescription': 'Clear',
                    }
                },
                {
                    'properties': {
                        'timestamp': '2026-03-24T10:00:00Z',
                        'temperature': {'value': None},
                        'textDescription': 'N/A',
                    }
                },
            ]
        }

        with patch('weatherlab.forecast.asos.requests.get', return_value=FakeResponse(payload)) as get_mock:
            observations = fetch_station_observations('KDCA', date(2026, 3, 24))

        self.assertEqual([row['time_utc'] for row in observations], [
            '2026-03-24T09:00:00Z',
            '2026-03-24T10:00:00Z',
            '2026-03-24T11:00:00Z',
        ])
        self.assertAlmostEqual(observations[0]['temp_f'], 60.8)
        self.assertIsNone(observations[1]['temp_f'])
        self.assertEqual(observations[2]['conditions'], 'Sunny')

        self.assertEqual(get_mock.call_args.kwargs['params']['start'], '2026-03-24T04:00:00Z')
        self.assertEqual(get_mock.call_args.kwargs['params']['end'], '2026-03-25T03:59:59Z')

    def test_fetch_station_daily_high_returns_max_observation(self):
        observations = [
            {'time_utc': '2026-03-24T09:00:00Z', 'temp_f': 62.1, 'conditions': 'Clear'},
            {'time_utc': '2026-03-24T10:00:00Z', 'temp_f': 67.8, 'conditions': 'Sunny'},
            {'time_utc': '2026-03-24T11:00:00Z', 'temp_f': 65.2, 'conditions': 'Sunny'},
        ]

        with patch('weatherlab.forecast.asos.fetch_station_observations', return_value=observations):
            observed_high = fetch_station_daily_high('KPHL', date(2026, 3, 24))

        self.assertEqual(observed_high, 67.8)

    def test_fetch_morning_validation_scores_tracking_forecast_high(self):
        observations = [
            {'time_utc': '2026-03-24T12:00:00Z', 'temp_f': 49.2, 'conditions': 'Clear'},
            {'time_utc': '2026-03-24T13:00:00Z', 'temp_f': 50.1, 'conditions': 'Sunny'},
        ]

        with patch('weatherlab.forecast.asos.fetch_station_forecast', return_value={'today_high_f': 52, 'tomorrow_high_f': 61, 'sky_condition': 'Sunny'}):
            with patch('weatherlab.forecast.asos.fetch_station_observations', return_value=observations):
                with patch('weatherlab.forecast.asos._now_utc', return_value=datetime(2026, 3, 24, 14, 0, tzinfo=UTC)):
                    validation = fetch_morning_validation('KDCA')

        self.assertEqual(validation['forecast_confidence'], 'high')
        self.assertEqual(validation['forecast_high_f'], 52)
        self.assertEqual(validation['observed_max_so_far_f'], 50.1)
        self.assertIn('tracking within', validation['note'])

    def test_fetch_morning_validation_scores_low_when_station_runs_hot(self):
        observations = [
            {'time_utc': '2026-03-24T14:00:00Z', 'temp_f': 60.4, 'conditions': 'Sunny'},
            {'time_utc': '2026-03-24T15:00:00Z', 'temp_f': 61.1, 'conditions': 'Sunny'},
        ]

        with patch('weatherlab.forecast.asos.fetch_station_forecast', return_value={'today_high_f': 52, 'tomorrow_high_f': 61, 'sky_condition': 'Sunny'}):
            with patch('weatherlab.forecast.asos.fetch_station_observations', return_value=observations):
                with patch('weatherlab.forecast.asos._now_utc', return_value=datetime(2026, 3, 24, 15, 0, tzinfo=UTC)):
                    validation = fetch_morning_validation('KDCA')

        self.assertEqual(validation['forecast_confidence'], 'low')
        self.assertEqual(validation['obs_count'], 2)
        self.assertIn('already exceeds', validation['note'])

    def test_fetch_morning_validation_returns_unknown_with_no_obs(self):
        with patch('weatherlab.forecast.asos.fetch_station_forecast', return_value={'today_high_f': 58, 'tomorrow_high_f': 64, 'sky_condition': 'Cloudy'}):
            with patch('weatherlab.forecast.asos.fetch_station_observations', return_value=[]):
                with patch('weatherlab.forecast.asos._now_utc', return_value=datetime(2026, 3, 24, 15, 0, tzinfo=UTC)):
                    validation = fetch_morning_validation('KPHL')

        self.assertEqual(validation['forecast_confidence'], 'unknown')
        self.assertIsNone(validation['observed_max_so_far_f'])
        self.assertEqual(validation['obs_count'], 0)


if __name__ == '__main__':
    unittest.main()
