from __future__ import annotations

from datetime import UTC, datetime
import importlib.util
from pathlib import Path
import unittest


SCRIPT_PATH = Path(__file__).resolve().parents[1] / 'scripts' / 'macro_scan.py'
SPEC = importlib.util.spec_from_file_location('macro_scan', SCRIPT_PATH)
assert SPEC is not None
macro_scan = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(macro_scan)


class MacroScanTests(unittest.TestCase):
    def test_filter_selects_low_priced_liquid_markets(self):
        now = datetime(2026, 4, 26, 12, 0, tzinfo=UTC)
        markets = [
            {
                'ticker': 'KXCPI-26MAY',
                'title': 'Will CPI be below 3.0%',
                'yes_ask': 0.15,
                'volume': 501,
                'close_time': '2026-04-28T12:00:00Z',
            },
            {
                'ticker': 'KXFED-26MAY',
                'title': 'Will the Fed cut rates?',
                'yes_ask': 16,
                'volume': 10_000,
                'close_time': '2026-04-27T12:00:00Z',
            },
            {
                'ticker': 'KXGDP-26MAY',
                'title': 'Will GDP be negative?',
                'yes_ask': 0.10,
                'volume': 500,
                'close_time': '2026-04-29T12:00:00Z',
            },
            {
                'ticker': 'KXJOBS-26MAY',
                'title': 'Will payrolls exceed 300k?',
                'yes_ask': 14,
                'volume': '750',
                'close_time': datetime(2026, 4, 30, 0, 0, tzinfo=UTC),
            },
        ]

        candidates = macro_scan.filter_coldmath_candidates(markets, now_utc=now)

        self.assertEqual([row['ticker'] for row in candidates], ['KXJOBS-26MAY', 'KXCPI-26MAY'])
        self.assertEqual(candidates[0]['yes_ask'], 0.14)
        self.assertEqual(candidates[0]['volume'], 750.0)
        self.assertEqual(candidates[0]['days_to_close'], 3.5)
        self.assertEqual(candidates[1]['days_to_close'], 2.0)

    def test_report_formatting_is_scannable_and_non_recommendational(self):
        report = macro_scan.format_macro_report(
            [
                {
                    'ticker': 'KXCPI-26MAY',
                    'title': 'Will CPI be below 3.0%?',
                    'yes_ask': 0.15,
                    'volume': 1250.0,
                    'days_to_close': 2.0,
                }
            ],
            scan_time_utc=datetime(2026, 4, 26, 12, 0, tzinfo=UTC),
        )

        self.assertIn('MACRO KALSHI SCAN - 2026-04-26T12:00:00Z', report)
        self.assertIn('Filter: yes_ask <= 15c and volume > 500', report)
        self.assertIn('Human review only. No recommendations generated.', report)
        self.assertIn('- KXCPI-26MAY | Will CPI be below 3.0%?', report)
        self.assertIn('yes_ask=15c | volume=1,250 | days_to_close=2.0', report)

    def test_empty_report_mentions_no_candidates(self):
        report = macro_scan.format_macro_report(
            [],
            scan_time_utc=datetime(2026, 4, 26, 12, 0, tzinfo=UTC),
        )

        self.assertIn('No low-priced liquid open markets found.', report)


if __name__ == '__main__':
    unittest.main()
