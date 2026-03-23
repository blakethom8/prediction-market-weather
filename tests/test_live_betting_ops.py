import tempfile
import unittest
from datetime import UTC, date, datetime
from pathlib import Path

from weatherlab.build.bootstrap import bootstrap
from weatherlab.build.registry_loader import load_all_registries
from weatherlab.build.training_rows import materialize_training_rows
from weatherlab.db import connect
from weatherlab.ingest.contracts import ingest_contract
from weatherlab.ingest.market_snapshots import ingest_market_snapshot
from weatherlab.ingest.open_meteo import ingest_open_meteo_daily_payload
from weatherlab.ingest.settlement_observations import ingest_settlement_observation
from weatherlab.live.persistence import replace_strategy_proposals
from weatherlab.ops.paper_bets import create_paper_bet, settle_paper_bet
from weatherlab.ops.strategy import create_strategy_session, populate_strategy_market_board


class LiveBettingOpsTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / 'live_betting.duckdb'
        bootstrap(db_path=self.db_path)
        load_all_registries(db_path=self.db_path)

    def tearDown(self):
        self.tmpdir.cleanup()

    def _seed_training_row(self):
        ingest_contract(
            market_ticker='TEST_NYC_54',
            event_ticker='TEST_EVENT',
            title='Will the high temp in NYC be above 54 on Mar 23, 2026?',
            close_time_utc=datetime(2026, 3, 23, 16, 0, tzinfo=UTC),
            settlement_time_utc=datetime(2026, 3, 24, 12, 0, tzinfo=UTC),
            db_path=self.db_path,
        )
        forecast_id = ingest_open_meteo_daily_payload(
            payload={
                'daily': {
                    'time': ['2026-03-23'],
                    'temperature_2m_max': [56.0],
                    'temperature_2m_min': [43.0],
                    'precipitation_probability_max': [20],
                }
            },
            city_id='nyc',
            target_date_local=date(2026, 3, 23),
            thresholds=[54.0],
            fetched_at_utc=datetime(2026, 3, 23, 9, 0, tzinfo=UTC),
            db_path=self.db_path,
        )
        ingest_market_snapshot(
            market_ticker='TEST_NYC_54',
            ts_utc=datetime(2026, 3, 23, 10, 0, tzinfo=UTC),
            price_yes_bid=0.42,
            price_yes_ask=0.45,
            price_no_bid=0.54,
            price_no_ask=0.58,
            last_price=0.44,
            volume=100,
            open_interest=25,
            minutes_to_close=360,
            db_path=self.db_path,
        )
        ingest_settlement_observation(
            source='nws-cli',
            station_id='KNYC',
            city_id='nyc',
            market_date_local=date(2026, 3, 23),
            observed_high_temp_f=57.0,
            report_published_at_utc=datetime(2026, 3, 24, 10, 0, tzinfo=UTC),
            db_path=self.db_path,
        )
        materialize_training_rows(db_path=self.db_path)
        return forecast_id

    def test_strategy_board_and_paper_bet_flow(self):
        forecast_id = self._seed_training_row()
        strategy_id = create_strategy_session(
            strategy_date_local=date(2026, 3, 23),
            thesis='Compare all available daily temperature bets before selecting one.',
            focus_cities=['nyc'],
            selection_framework={'goal': 'marginal daily gains', 'mode': 'paper'},
            db_path=self.db_path,
        )
        count = populate_strategy_market_board(
            strategy_id=strategy_id,
            strategy_date_local=date(2026, 3, 23),
            focus_cities=['nyc'],
            db_path=self.db_path,
        )
        self.assertEqual(count, 1)

        con = connect(db_path=self.db_path)
        try:
            board_row = con.execute(
                '''
                select
                    board_entry_id,
                    market_ticker,
                    city_id,
                    market_date_local,
                    price_yes_ask,
                    fair_prob,
                    edge_vs_ask,
                    candidate_rank,
                    candidate_bucket
                from ops.strategy_market_board
                where strategy_id = ?
                ''',
                [strategy_id],
            ).fetchone()
        finally:
            con.close()

        persisted = replace_strategy_proposals(
            strategy_id=strategy_id,
            proposals=[
                {
                    'board_entry_id': board_row[0],
                    'market_ticker': board_row[1],
                    'city_id': board_row[2],
                    'market_date_local': board_row[3].isoformat(),
                    'proposal_status': 'pending_review',
                    'side': 'BUY_YES',
                    'market_price': board_row[4],
                    'target_price': board_row[4],
                    'target_quantity': 10,
                    'fair_prob': board_row[5],
                    'perceived_edge': board_row[6],
                    'candidate_rank': board_row[7],
                    'candidate_bucket': board_row[8],
                    'forecast_snapshot_id': forecast_id,
                    'strategy_variant': 'baseline',
                    'scenario_label': 'live',
                    'thesis': 'Compare all available daily temperature bets before selecting one.',
                    'rationale_summary': 'Forecast runs warmer than market bucket.',
                    'rationale_json': {'edge_vs_ask': board_row[6]},
                    'context_json': {'source': 'test'},
                }
            ],
            db_path=self.db_path,
        )
        proposal_id = persisted[0]['proposal_id']

        paper_bet_id = create_paper_bet(
            strategy_id=strategy_id,
            market_ticker='TEST_NYC_54',
            side='BUY_YES',
            limit_price=0.45,
            quantity=10,
            proposal_id=proposal_id,
            rationale_summary=None,
            db_path=self.db_path,
        )
        settle_paper_bet(
            paper_bet_id=paper_bet_id,
            outcome_label='YES',
            review={'lesson': 'Good edge capture on cool-day board.'},
            db_path=self.db_path,
        )

        con = connect(db_path=self.db_path)
        try:
            board_row = con.execute(
                'select market_ticker, candidate_bucket from ops.strategy_market_board where strategy_id = ?',
                [strategy_id],
            ).fetchone()
            bet_row = con.execute(
                '''
                select status, realized_pnl, outcome_label, proposal_id
                from ops.paper_bets
                where paper_bet_id = ?
                ''',
                [paper_bet_id],
            ).fetchone()
            proposal_row = con.execute(
                '''
                select proposal_status, linked_paper_bet_id
                from ops.bet_proposals
                where proposal_id = ?
                ''',
                [proposal_id],
            ).fetchone()
            review_row = con.execute(
                '''
                select kalshi_outcome_label, lesson_summary
                from ops.paper_bet_reviews
                where paper_bet_id = ?
                ''',
                [paper_bet_id],
            ).fetchone()
        finally:
            con.close()

        self.assertEqual(board_row[0], 'TEST_NYC_54')
        self.assertIn(board_row[1], {'priority', 'watch', 'pass'})
        self.assertEqual(bet_row[0], 'closed')
        self.assertEqual(bet_row[2], 'YES')
        self.assertAlmostEqual(bet_row[1], 5.5)
        self.assertEqual(bet_row[3], proposal_id)
        self.assertEqual(proposal_row[0], 'settled')
        self.assertEqual(proposal_row[1], paper_bet_id)
        self.assertEqual(review_row[0], 'YES')
        self.assertEqual(review_row[1], 'Good edge capture on cool-day board.')


if __name__ == '__main__':
    unittest.main()
