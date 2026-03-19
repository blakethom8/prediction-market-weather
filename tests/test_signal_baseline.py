import unittest

from signal import choose_action, compute_edge


class SignalBaselineTests(unittest.TestCase):
    def test_compute_edge_against_ask(self):
        self.assertAlmostEqual(compute_edge(0.62, 0.51), 0.11)

    def test_choose_action_buy_yes(self):
        action = choose_action(fair_prob=0.62, tradable_yes_ask=0.5, min_edge=0.05)
        self.assertEqual(action, "BUY_YES")

    def test_choose_action_abstains_when_edge_small(self):
        action = choose_action(fair_prob=0.53, tradable_yes_ask=0.5, min_edge=0.05)
        self.assertEqual(action, "NO_TRADE")


if __name__ == "__main__":
    unittest.main()
