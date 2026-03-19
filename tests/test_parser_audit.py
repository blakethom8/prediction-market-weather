import unittest

from weatherlab.parse.audit import audit_titles, summarize_audit


class ParserAuditTests(unittest.TestCase):
    def test_audit_titles_and_summary(self):
        rows = [
            {'market_ticker': 'A', 'title': 'Will the high temp in NYC be >78° on Nov 23, 2025?'},
            {'market_ticker': 'B', 'title': 'Will the high temperature in Miami be below 80 on Sep 18, 2026?'},
            {'market_ticker': 'C', 'title': 'Will it be windy in Boston tomorrow?'},
        ]
        audited = audit_titles(rows)
        summary = summarize_audit(audited)

        self.assertEqual(len(audited), 3)
        self.assertEqual(summary['total'], 3)
        self.assertEqual(summary['parsed'], 2)
        self.assertEqual(summary['failed'], 1)
        self.assertEqual(audited[0]['city_id'], 'nyc')
        self.assertEqual(audited[1]['operator'], '<=')
        self.assertEqual(audited[2]['parse_status'], 'failed')


if __name__ == '__main__':
    unittest.main()
