"""Tests for Kalshi vs Polymarket arbitrage scanner."""
import unittest
from datetime import UTC, datetime

from scripts.arb_scan import filter_arb_candidates, format_arb_report, match_markets


def _kalshi(ticker: str, yes_ask: float) -> dict:
    return {'ticker': ticker, 'yes_ask': yes_ask}


def _poly(question: str, yes_price: str, volume: float = 1000.0, slug: str = 'test-slug') -> dict:
    import json
    no_price = str(round(1.0 - float(yes_price), 4))
    return {
        'question': question,
        'outcomePrices': json.dumps([yes_price, no_price]),
        'volume': volume,
        'slug': slug,
        'endDate': '2026-12-31T00:00:00Z',
    }


class ArbScanMatchTests(unittest.TestCase):
    def test_match_cpi_markets_by_keyword(self):
        kalshi = [_kalshi('KXCPI-26MAY-T0.8', 0.57)]
        poly = [_poly('Will CPI exceed 0.8% MoM in May 2026?', '0.50', volume=2000, slug='cpi-may')]
        pairs = match_markets(kalshi, poly)
        self.assertEqual(len(pairs), 1)
        self.assertEqual(pairs[0]['kalshi_ticker'], 'KXCPI-26MAY-T0.8')
        self.assertAlmostEqual(pairs[0]['kalshi_yes'], 0.57)
        self.assertAlmostEqual(pairs[0]['poly_yes'], 0.50)
        self.assertAlmostEqual(pairs[0]['diff'], 0.07, places=4)
        self.assertEqual(pairs[0]['diff_direction'], 'kalshi_higher')

    def test_no_match_when_keywords_dont_overlap(self):
        kalshi = [_kalshi('KXHIGHMIA-26APR27-B84.5', 0.39)]
        poly = [_poly('Will the Fed cut rates in May?', '0.30')]
        pairs = match_markets(kalshi, poly)
        # weather keywords may match - that's fine; just confirm no cross-category bleed
        for p in pairs:
            self.assertNotIn('Fed', p['poly_question'])

    def test_filter_removes_small_diff(self):
        pairs = [
            {'diff': 0.03, 'poly_volume': 1000, 'diff_direction': 'kalshi_higher',
             'kalshi_ticker': 'K1', 'kalshi_yes': 0.53, 'poly_slug': 'p1',
             'poly_question': 'Q1', 'poly_yes': 0.50, 'days_to_close': 10.0},
        ]
        self.assertEqual(filter_arb_candidates(pairs, min_diff=0.05), [])

    def test_filter_removes_low_volume(self):
        pairs = [
            {'diff': 0.10, 'poly_volume': 200, 'diff_direction': 'poly_higher',
             'kalshi_ticker': 'K2', 'kalshi_yes': 0.40, 'poly_slug': 'p2',
             'poly_question': 'Q2', 'poly_yes': 0.50, 'days_to_close': 5.0},
        ]
        self.assertEqual(filter_arb_candidates(pairs, min_volume=500), [])

    def test_filter_keeps_qualifying_candidates(self):
        pairs = [
            {'diff': 0.08, 'poly_volume': 1500, 'diff_direction': 'kalshi_higher',
             'kalshi_ticker': 'KXCPI-26MAY', 'kalshi_yes': 0.58, 'poly_slug': 'cpi-may',
             'poly_question': 'CPI question', 'poly_yes': 0.50, 'days_to_close': 14.0},
        ]
        result = filter_arb_candidates(pairs, min_diff=0.05, min_volume=500)
        self.assertEqual(len(result), 1)

    def test_report_no_candidates(self):
        report = format_arb_report([], scan_time_utc=datetime(2026, 4, 26, 18, 0, tzinfo=UTC))
        self.assertIn('KALSHI vs POLYMARKET ARB SCAN', report)
        self.assertIn('No arbitrage candidates found', report)

    def test_report_formats_candidates(self):
        candidates = [
            {'diff': 0.08, 'diff_direction': 'kalshi_higher', 'poly_volume': 1500,
             'kalshi_ticker': 'KXCPI-26MAY-T0.8', 'kalshi_yes': 0.58,
             'poly_slug': 'cpi-may-0-8', 'poly_question': 'Will CPI exceed 0.8%?',
             'poly_yes': 0.50, 'days_to_close': 14.0},
        ]
        report = format_arb_report(candidates, scan_time_utc=datetime(2026, 4, 26, 18, 0, tzinfo=UTC))
        self.assertIn('Kalshi higher', report)
        self.assertIn('KXCPI-26MAY-T0.8', report)
        self.assertIn('cpi-may-0-8', report)
        self.assertIn('diff=8c', report)


if __name__ == '__main__':
    unittest.main()
