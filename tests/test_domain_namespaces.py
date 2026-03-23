import unittest

from weatherlab.evaluation import EvalRow as LegacyEvalRow
from weatherlab.evaluation import score_row as legacy_score_row
from weatherlab.forecast_archive_plan import get_focus_city_archive_plan as legacy_get_focus_city_archive_plan
from weatherlab.replay import replay_decision_for_market as legacy_replay_decision_for_market
from weatherlab.research.archive_plan import get_focus_city_archive_plan
from weatherlab.research.evaluation import EvalRow, score_row
from weatherlab.research.replay import replay_decision_for_market


class DomainNamespaceTests(unittest.TestCase):
    def test_research_namespace_reexports_existing_research_helpers(self):
        self.assertIs(EvalRow, LegacyEvalRow)
        self.assertIs(score_row, legacy_score_row)
        self.assertIs(get_focus_city_archive_plan, legacy_get_focus_city_archive_plan)
        self.assertIs(replay_decision_for_market, legacy_replay_decision_for_market)


if __name__ == '__main__':
    unittest.main()
