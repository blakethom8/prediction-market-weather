import unittest
from datetime import date

from src.weatherlab.parse.contract_parser import parse_temperature_contract


class ContractParserTests(unittest.TestCase):
    def test_parses_between_bucket(self):
        parsed = parse_temperature_contract(
            "TEST1",
            "Will the high temperature in Chicago be between 40 and 44 degrees on Mar 18, 2026?",
        )
        self.assertEqual(parsed.city_id, "chi")
        self.assertEqual(parsed.measure, "daily_high_temp_f")
        self.assertEqual(parsed.operator, "between")
        self.assertEqual(parsed.threshold_low_f, 40.0)
        self.assertEqual(parsed.threshold_high_f, 44.0)
        self.assertEqual(parsed.market_date_local, date(2026, 3, 18))
        self.assertEqual(parsed.parse_status, "parsed")

    def test_parses_hyphen_range_bucket(self):
        parsed = parse_temperature_contract(
            "TEST_RANGE",
            "Will the high temp in NYC be 62-63° on Nov 5, 2025?",
        )
        self.assertEqual(parsed.city_id, "nyc")
        self.assertEqual(parsed.operator, "between")
        self.assertEqual(parsed.threshold_low_f, 62.0)
        self.assertEqual(parsed.threshold_high_f, 63.0)
        self.assertEqual(parsed.market_date_local, date(2025, 11, 5))
        self.assertEqual(parsed.parse_status, "parsed")

    def test_parses_threshold_bucket(self):
        parsed = parse_temperature_contract(
            "TEST2",
            "Will the high temp in New York be above 70 on Mar 18, 2026?",
        )
        self.assertEqual(parsed.city_id, "nyc")
        self.assertEqual(parsed.operator, ">=")
        self.assertEqual(parsed.threshold_low_f, 70.0)
        self.assertEqual(parsed.market_date_local, date(2026, 3, 18))
        self.assertEqual(parsed.parse_status, "parsed")

    def test_parses_or_higher_threshold(self):
        parsed = parse_temperature_contract(
            "TEST3",
            "Will the high temperature in Dallas be 65° or higher on Apr 2, 2026?",
        )
        self.assertEqual(parsed.city_id, "dal")
        self.assertEqual(parsed.operator, ">=")
        self.assertEqual(parsed.threshold_low_f, 65.0)
        self.assertEqual(parsed.market_date_local, date(2026, 4, 2))

    def test_parses_less_than_threshold(self):
        parsed = parse_temperature_contract(
            "TEST4",
            "Will the high temperature in Miami be below 80 on Sep 18, 2026?",
        )
        self.assertEqual(parsed.city_id, "mia")
        self.assertEqual(parsed.operator, "<=")
        self.assertEqual(parsed.threshold_low_f, 80.0)
        self.assertEqual(parsed.market_date_local, date(2026, 9, 18))
        self.assertEqual(parsed.parse_status, "parsed")

    def test_parses_symbol_threshold(self):
        parsed = parse_temperature_contract(
            "TEST5",
            "Will the high temp in Austin be >78° on Nov 23, 2025?",
        )
        self.assertEqual(parsed.city_id, "aus")
        self.assertEqual(parsed.operator, ">=")
        self.assertEqual(parsed.threshold_low_f, 78.0)
        self.assertEqual(parsed.market_date_local, date(2025, 11, 23))

    def test_fails_cleanly_for_unsupported_title(self):
        parsed = parse_temperature_contract("TEST6", "Will it be windy in Boston tomorrow?")
        self.assertEqual(parsed.parse_status, "failed")
        self.assertIsNone(parsed.operator)
        self.assertIsNone(parsed.threshold_low_f)


if __name__ == "__main__":
    unittest.main()
