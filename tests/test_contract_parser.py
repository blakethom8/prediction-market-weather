import unittest

from src.weatherlab.parse.contract_parser import parse_temperature_contract


class ContractParserTests(unittest.TestCase):
    def test_parses_between_bucket(self):
        parsed = parse_temperature_contract(
            "TEST1",
            "Will the high temperature in Chicago be between 40 and 44 degrees on Mar 18?",
        )
        self.assertEqual(parsed.city_id, "chi")
        self.assertEqual(parsed.measure, "daily_high_temp_f")
        self.assertEqual(parsed.operator, "between")
        self.assertEqual(parsed.threshold_low_f, 40.0)
        self.assertEqual(parsed.threshold_high_f, 44.0)
        self.assertEqual(parsed.parse_status, "parsed")

    def test_parses_threshold_bucket(self):
        parsed = parse_temperature_contract(
            "TEST2",
            "Will the high temp in New York be above 70 on Mar 18?",
        )
        self.assertEqual(parsed.city_id, "nyc")
        self.assertEqual(parsed.operator, ">=")
        self.assertEqual(parsed.threshold_low_f, 70.0)
        self.assertEqual(parsed.parse_status, "parsed")

    def test_fails_cleanly_for_unsupported_title(self):
        parsed = parse_temperature_contract("TEST3", "Will it be windy in Boston tomorrow?")
        self.assertEqual(parsed.parse_status, "failed")
        self.assertIsNone(parsed.operator)
        self.assertIsNone(parsed.threshold_low_f)


if __name__ == "__main__":
    unittest.main()
