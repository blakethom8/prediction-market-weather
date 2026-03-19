import unittest

from weatherlab.evaluation import EvalRow, score_row


class EvaluationTests(unittest.TestCase):
    def test_score_row_returns_positive_pnl_for_correct_buy_yes(self):
        scored = score_row(EvalRow("TEST_A", 0.62, 0.50, 1))

        self.assertEqual(scored["action"], "BUY_YES")
        self.assertAlmostEqual(scored["edge"], 0.12)
        self.assertAlmostEqual(scored["pnl"], 0.50)

    def test_score_row_keeps_zero_pnl_when_no_trade(self):
        scored = score_row(EvalRow("TEST_B", 0.53, 0.50, 0))

        self.assertEqual(scored["action"], "NO_TRADE")
        self.assertAlmostEqual(scored["edge"], 0.03)
        self.assertEqual(scored["pnl"], 0.0)


if __name__ == "__main__":
    unittest.main()
