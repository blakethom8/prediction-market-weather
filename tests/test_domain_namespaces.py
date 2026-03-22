import unittest

from weatherlab.evaluation import EvalRow as LegacyEvalRow
from weatherlab.evaluation import score_row as legacy_score_row
from weatherlab.forecast_archive_plan import get_focus_city_archive_plan as legacy_get_focus_city_archive_plan
from weatherlab.live.persistence import create_paper_bet, create_strategy_session, populate_strategy_market_board, settle_paper_bet
from weatherlab.live.persistence import update_strategy_approval
from weatherlab.ops.paper_bets import create_paper_bet as legacy_create_paper_bet
from weatherlab.ops.paper_bets import settle_paper_bet as legacy_settle_paper_bet
from weatherlab.ops.strategy import create_strategy_session as legacy_create_strategy_session
from weatherlab.ops.strategy import populate_strategy_market_board as legacy_populate_strategy_market_board
from weatherlab.ops.strategy import update_strategy_approval as legacy_update_strategy_approval
from weatherlab.replay import replay_decision_for_market as legacy_replay_decision_for_market
from weatherlab.research.archive_plan import get_focus_city_archive_plan
from weatherlab.research.evaluation import EvalRow, score_row
from weatherlab.research.replay import replay_decision_for_market


class DomainNamespaceTests(unittest.TestCase):
    def test_live_persistence_and_legacy_ops_imports_point_to_same_functions(self):
        self.assertIs(create_strategy_session, legacy_create_strategy_session)
        self.assertIs(populate_strategy_market_board, legacy_populate_strategy_market_board)
        self.assertIs(update_strategy_approval, legacy_update_strategy_approval)
        self.assertIs(create_paper_bet, legacy_create_paper_bet)
        self.assertIs(settle_paper_bet, legacy_settle_paper_bet)

    def test_research_namespace_reexports_existing_research_helpers(self):
        self.assertIs(EvalRow, LegacyEvalRow)
        self.assertIs(score_row, legacy_score_row)
        self.assertIs(get_focus_city_archive_plan, legacy_get_focus_city_archive_plan)
        self.assertIs(replay_decision_for_market, legacy_replay_decision_for_market)


if __name__ == '__main__':
    unittest.main()
