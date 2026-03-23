import json
import tempfile
import unittest
from datetime import UTC, date, datetime, time
from pathlib import Path

from weatherlab.build.bootstrap import bootstrap
from weatherlab.build.registry_loader import load_all_registries
from weatherlab.db import connect
from weatherlab.ingest.contracts import ingest_contract
from weatherlab.live.persistence import create_paper_bet, create_strategy_session, replace_strategy_proposals, settle_paper_bet
from weatherlab.live.queries import get_history_snapshot
from weatherlab.live.workflow import apply_strategy_review
from weatherlab.utils.ids import new_id

try:
    from fastapi.testclient import TestClient
    from weatherlab.live.web import create_app
except ImportError:  # pragma: no cover - exercised only when web deps are absent
    TestClient = None
    create_app = None


class HistoricalSeedMixin:
    def _seed_contract(self, *, market_ticker: str, title: str, event_ticker: str, market_date_local: date) -> None:
        ingest_contract(
            market_ticker=market_ticker,
            event_ticker=event_ticker,
            title=title,
            close_time_utc=datetime.combine(market_date_local, time(16, 0), tzinfo=UTC),
            settlement_time_utc=datetime.combine(market_date_local, time(20, 0), tzinfo=UTC),
            db_path=self.db_path,
        )

    def _insert_board_rows(
        self,
        *,
        strategy_id: str,
        strategy_date_local: date,
        board_rows: list[dict[str, object]],
    ) -> list[dict[str, object]]:
        captured_at_utc = datetime.combine(strategy_date_local, time(9, 30), tzinfo=UTC)
        con = connect(db_path=self.db_path)
        try:
            persisted_rows: list[dict[str, object]] = []
            for row in board_rows:
                board_entry_id = new_id('board')
                persisted_rows.append(
                    {
                        **row,
                        'board_entry_id': board_entry_id,
                        'market_date_local': strategy_date_local,
                    }
                )
                con.execute(
                    '''
                    insert into ops.strategy_market_board (
                        board_entry_id, strategy_id, market_ticker, market_title, captured_at_utc,
                        city_id, market_date_local, forecast_snapshot_id, minutes_to_close,
                        price_yes_mid, price_yes_ask, price_yes_bid, fair_prob, edge_vs_mid,
                        edge_vs_ask, candidate_rank, candidate_bucket, board_notes_json
                    ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''',
                    [
                        board_entry_id,
                        strategy_id,
                        row['market_ticker'],
                        row['market_title'],
                        captured_at_utc,
                        row['city_id'],
                        strategy_date_local,
                        None,
                        row['minutes_to_close'],
                        row['price_yes_mid'],
                        row['price_yes_ask'],
                        row['price_yes_bid'],
                        row['fair_prob'],
                        row['edge_vs_mid'],
                        row['edge_vs_ask'],
                        row['candidate_rank'],
                        row['candidate_bucket'],
                        json.dumps({'source': 'history-test'}),
                    ],
                )
            con.execute(
                '''
                update ops.strategy_sessions
                set board_generated_at_utc = ?,
                    board_market_count = ?,
                    board_city_count = ?
                where strategy_id = ?
                ''',
                [
                    captured_at_utc,
                    len(persisted_rows),
                    len({str(row['city_id']) for row in persisted_rows}),
                    strategy_id,
                ],
            )
        finally:
            con.close()
        return persisted_rows

    def _build_proposals(
        self,
        *,
        board_rows: list[dict[str, object]],
        strategy_variant: str,
        scenario_label: str,
        thesis: str,
    ) -> list[dict[str, object]]:
        proposals: list[dict[str, object]] = []
        for row in board_rows:
            if row['candidate_bucket'] != 'priority':
                continue
            proposals.append(
                {
                    'board_entry_id': row['board_entry_id'],
                    'market_ticker': row['market_ticker'],
                    'city_id': row['city_id'],
                    'market_date_local': row['market_date_local'].isoformat(),
                    'proposal_status': 'pending_review',
                    'side': 'BUY_YES',
                    'market_price': row['price_yes_ask'],
                    'target_price': row['price_yes_ask'],
                    'target_quantity': 10,
                    'fair_prob': row['fair_prob'],
                    'perceived_edge': row['edge_vs_ask'],
                    'candidate_rank': row['candidate_rank'],
                    'candidate_bucket': row['candidate_bucket'],
                    'forecast_snapshot_id': None,
                    'strategy_variant': strategy_variant,
                    'scenario_label': scenario_label,
                    'thesis': thesis,
                    'rationale_summary': f"{row['market_ticker']} led the board on stored edge.",
                    'rationale_json': {'edge_vs_ask': row['edge_vs_ask']},
                    'context_json': {'source': 'history-test'},
                }
            )
        return proposals

    def _backdate_history(
        self,
        *,
        strategy_id: str,
        proposal_id: str | None,
        strategy_date_local: date,
        approval_status: str,
        paper_bet_id: str | None = None,
        closed_at_utc: datetime | None = None,
    ) -> None:
        created_at_utc = datetime.combine(strategy_date_local, time(9, 0), tzinfo=UTC)
        proposed_at_utc = datetime.combine(strategy_date_local, time(9, 35), tzinfo=UTC)
        reviewed_at_utc = datetime.combine(strategy_date_local, time(10, 0), tzinfo=UTC)
        paper_created_at_utc = datetime.combine(strategy_date_local, time(10, 15), tzinfo=UTC)

        con = connect(db_path=self.db_path)
        try:
            con.execute(
                '''
                update ops.strategy_sessions
                set created_at_utc = ?,
                    last_reviewed_at_utc = ?,
                    approved_at_utc = ?
                where strategy_id = ?
                ''',
                [
                    created_at_utc,
                    reviewed_at_utc,
                    reviewed_at_utc if approval_status == 'approved' else None,
                    strategy_id,
                ],
            )
            con.execute(
                'update ops.strategy_market_board set captured_at_utc = ? where strategy_id = ?',
                [datetime.combine(strategy_date_local, time(9, 30), tzinfo=UTC), strategy_id],
            )
            con.execute(
                'update ops.bet_proposals set proposed_at_utc = ? where strategy_id = ?',
                [proposed_at_utc, strategy_id],
            )
            con.execute(
                "update ops.bet_proposal_events set event_at_utc = ? where strategy_id = ? and decision = 'proposed'",
                [proposed_at_utc, strategy_id],
            )
            con.execute(
                'update ops.strategy_review_events set reviewed_at_utc = ? where strategy_id = ?',
                [reviewed_at_utc, strategy_id],
            )
            con.execute(
                '''
                update ops.bet_proposal_events
                set event_at_utc = ?
                where strategy_id = ?
                  and resulting_status in ('approved', 'adjustments_requested', 'rejected')
                ''',
                [reviewed_at_utc, strategy_id],
            )
            if paper_bet_id is not None:
                con.execute(
                    'update ops.paper_bets set created_at_utc = ? where paper_bet_id = ?',
                    [paper_created_at_utc, paper_bet_id],
                )
                if proposal_id is not None:
                    con.execute(
                        '''
                        update ops.bet_proposal_events
                        set event_at_utc = ?
                        where proposal_id = ?
                          and decision = 'converted_to_paper'
                        ''',
                        [paper_created_at_utc, proposal_id],
                    )
            if closed_at_utc is not None and paper_bet_id is not None:
                con.execute(
                    'update ops.paper_bets set closed_at_utc = ? where paper_bet_id = ?',
                    [closed_at_utc, paper_bet_id],
                )
                con.execute(
                    'update ops.paper_bet_reviews set reviewed_at_utc = ? where paper_bet_id = ?',
                    [closed_at_utc, paper_bet_id],
                )
                if proposal_id is not None:
                    con.execute(
                        '''
                        update ops.bet_proposal_events
                        set event_at_utc = ?
                        where proposal_id = ?
                          and decision = 'settled'
                        ''',
                        [closed_at_utc, proposal_id],
                    )
        finally:
            con.close()

    def _create_historical_session(
        self,
        *,
        strategy_date_local: date,
        strategy_variant: str,
        scenario_label: str,
        thesis: str,
        review_decision: str,
        review_reason: str,
        board_rows: list[dict[str, object]],
        paper_limit_price: float | None = None,
        paper_outcome: str | None = None,
        lesson_summary: str | None = None,
        settle_date_local: date | None = None,
    ) -> tuple[str, str | None, str | None]:
        strategy_id = create_strategy_session(
            strategy_date_local=strategy_date_local,
            thesis=thesis,
            research_focus_cities=['nyc', 'chi'],
            strategy_variant=strategy_variant,
            scenario_label=scenario_label,
            db_path=self.db_path,
        )
        persisted_board = self._insert_board_rows(
            strategy_id=strategy_id,
            strategy_date_local=strategy_date_local,
            board_rows=board_rows,
        )
        proposals = replace_strategy_proposals(
            strategy_id=strategy_id,
            proposals=self._build_proposals(
                board_rows=persisted_board,
                strategy_variant=strategy_variant,
                scenario_label=scenario_label,
                thesis=thesis,
            ),
            db_path=self.db_path,
        )
        apply_strategy_review(
            strategy_id=strategy_id,
            decision=review_decision,
            notes={'reason': review_reason},
            db_path=self.db_path,
        )

        proposal_id = proposals[0]['proposal_id'] if proposals else None
        paper_bet_id = None
        if proposal_id is not None and paper_limit_price is not None:
            paper_bet_id = create_paper_bet(
                strategy_id=strategy_id,
                market_ticker=str(proposals[0]['market_ticker']),
                side='BUY_YES',
                limit_price=paper_limit_price,
                quantity=10,
                proposal_id=proposal_id,
                db_path=self.db_path,
            )
            if paper_outcome is not None:
                settle_paper_bet(
                    paper_bet_id=paper_bet_id,
                    outcome_label=paper_outcome,
                    review={'lesson_summary': lesson_summary},
                    db_path=self.db_path,
                )

        closed_at_utc = (
            datetime.combine(settle_date_local, time(18, 0), tzinfo=UTC)
            if settle_date_local is not None
            else None
        )
        self._backdate_history(
            strategy_id=strategy_id,
            proposal_id=proposal_id,
            strategy_date_local=strategy_date_local,
            approval_status={
                'approve': 'approved',
                'adjust': 'adjustments_requested',
                'reject': 'rejected',
            }[review_decision],
            paper_bet_id=paper_bet_id,
            closed_at_utc=closed_at_utc,
        )
        return strategy_id, proposal_id, paper_bet_id

    def seed_history(self) -> None:
        self._seed_contract(
            market_ticker='HIST_NYC_54_0312',
            event_ticker='HIST_EVENT_NYC_0312',
            title='Will the high temp in NYC be above 54 on Mar 12, 2026?',
            market_date_local=date(2026, 3, 12),
        )
        self._seed_contract(
            market_ticker='HIST_CHI_58_0312',
            event_ticker='HIST_EVENT_CHI_0312',
            title='Will the high temp in Chicago be above 58 on Mar 12, 2026?',
            market_date_local=date(2026, 3, 12),
        )
        self._seed_contract(
            market_ticker='HIST_CHI_60_0319',
            event_ticker='HIST_EVENT_CHI_0319',
            title='Will the high temp in Chicago be above 60 on Mar 19, 2026?',
            market_date_local=date(2026, 3, 19),
        )
        self._seed_contract(
            market_ticker='HIST_NYC_55_0319',
            event_ticker='HIST_EVENT_NYC_0319',
            title='Will the high temp in NYC be above 55 on Mar 19, 2026?',
            market_date_local=date(2026, 3, 19),
        )
        self._seed_contract(
            market_ticker='HIST_LAX_72_0320',
            event_ticker='HIST_EVENT_LAX_0320',
            title='Will the high temp in Los Angeles be above 72 on Mar 20, 2026?',
            market_date_local=date(2026, 3, 20),
        )
        self._seed_contract(
            market_ticker='HIST_DAL_66_0320',
            event_ticker='HIST_EVENT_DAL_0320',
            title='Will the high temp in Dallas be above 66 on Mar 20, 2026?',
            market_date_local=date(2026, 3, 20),
        )
        self._seed_contract(
            market_ticker='HIST_NYC_56_0321',
            event_ticker='HIST_EVENT_NYC_0321',
            title='Will the high temp in NYC be above 56 on Mar 21, 2026?',
            market_date_local=date(2026, 3, 21),
        )
        self._seed_contract(
            market_ticker='HIST_LAX_70_0321',
            event_ticker='HIST_EVENT_LAX_0321',
            title='Will the high temp in Los Angeles be above 70 on Mar 21, 2026?',
            market_date_local=date(2026, 3, 21),
        )

        self.session_a = self._create_historical_session(
            strategy_date_local=date(2026, 3, 12),
            strategy_variant='baseline',
            scenario_label='cold_snap',
            thesis='Compare the full board before acting on the strongest cool-day edge.',
            review_decision='approve',
            review_reason='Clear full-board edge in NYC.',
            board_rows=[
                {
                    'market_ticker': 'HIST_NYC_54_0312',
                    'market_title': 'NYC priority setup',
                    'city_id': 'nyc',
                    'minutes_to_close': 90,
                    'price_yes_mid': 0.41,
                    'price_yes_ask': 0.43,
                    'price_yes_bid': 0.39,
                    'fair_prob': 0.54,
                    'edge_vs_mid': 0.13,
                    'edge_vs_ask': 0.11,
                    'candidate_rank': 1,
                    'candidate_bucket': 'priority',
                },
                {
                    'market_ticker': 'HIST_CHI_58_0312',
                    'market_title': 'Chicago watch setup',
                    'city_id': 'chi',
                    'minutes_to_close': 180,
                    'price_yes_mid': 0.35,
                    'price_yes_ask': 0.36,
                    'price_yes_bid': 0.34,
                    'fair_prob': 0.40,
                    'edge_vs_mid': 0.05,
                    'edge_vs_ask': 0.04,
                    'candidate_rank': 2,
                    'candidate_bucket': 'watch',
                },
            ],
            paper_limit_price=0.43,
            paper_outcome='YES',
            lesson_summary='Wait for the better entry after the full-board scan.',
            settle_date_local=date(2026, 3, 13),
        )

        self.session_b = self._create_historical_session(
            strategy_date_local=date(2026, 3, 19),
            strategy_variant='baseline-v2',
            scenario_label='warm_shift',
            thesis='Use the full board, but re-price warm-city edges before converting.',
            review_decision='adjust',
            review_reason='Need a later entry before converting.',
            board_rows=[
                {
                    'market_ticker': 'HIST_CHI_60_0319',
                    'market_title': 'Chicago adjusted priority setup',
                    'city_id': 'chi',
                    'minutes_to_close': 240,
                    'price_yes_mid': 0.60,
                    'price_yes_ask': 0.62,
                    'price_yes_bid': 0.58,
                    'fair_prob': 0.66,
                    'edge_vs_mid': 0.06,
                    'edge_vs_ask': 0.04,
                    'candidate_rank': 1,
                    'candidate_bucket': 'priority',
                },
                {
                    'market_ticker': 'HIST_NYC_55_0319',
                    'market_title': 'NYC watch setup',
                    'city_id': 'nyc',
                    'minutes_to_close': 360,
                    'price_yes_mid': 0.39,
                    'price_yes_ask': 0.40,
                    'price_yes_bid': 0.38,
                    'fair_prob': 0.44,
                    'edge_vs_mid': 0.05,
                    'edge_vs_ask': 0.04,
                    'candidate_rank': 2,
                    'candidate_bucket': 'watch',
                },
            ],
            paper_limit_price=0.58,
            paper_outcome='NO',
            lesson_summary='Wait for the better entry after the full-board scan.',
            settle_date_local=date(2026, 3, 20),
        )

        self.session_c = self._create_historical_session(
            strategy_date_local=date(2026, 3, 20),
            strategy_variant='baseline-v2',
            scenario_label='coast_compare',
            thesis='Record the pass cleanly when the slate does not justify paper exposure.',
            review_decision='reject',
            review_reason='No clean approval today.',
            board_rows=[
                {
                    'market_ticker': 'HIST_LAX_72_0320',
                    'market_title': 'Los Angeles rejected priority setup',
                    'city_id': 'lax',
                    'minutes_to_close': 600,
                    'price_yes_mid': 0.31,
                    'price_yes_ask': 0.33,
                    'price_yes_bid': 0.29,
                    'fair_prob': 0.45,
                    'edge_vs_mid': 0.14,
                    'edge_vs_ask': 0.12,
                    'candidate_rank': 1,
                    'candidate_bucket': 'priority',
                },
                {
                    'market_ticker': 'HIST_DAL_66_0320',
                    'market_title': 'Dallas pass setup',
                    'city_id': 'dal',
                    'minutes_to_close': 720,
                    'price_yes_mid': 0.55,
                    'price_yes_ask': 0.56,
                    'price_yes_bid': 0.54,
                    'fair_prob': 0.57,
                    'edge_vs_mid': 0.02,
                    'edge_vs_ask': 0.01,
                    'candidate_rank': 2,
                    'candidate_bucket': 'pass',
                },
            ],
        )

        self.session_d = self._create_historical_session(
            strategy_date_local=date(2026, 3, 21),
            strategy_variant='baseline-v3',
            scenario_label='carry_forward',
            thesis='Keep one high-conviction edge open while the next review cycle catches up.',
            review_decision='approve',
            review_reason='Good edge but keep exposure small.',
            board_rows=[
                {
                    'market_ticker': 'HIST_NYC_56_0321',
                    'market_title': 'NYC open priority setup',
                    'city_id': 'nyc',
                    'minutes_to_close': 900,
                    'price_yes_mid': 0.42,
                    'price_yes_ask': 0.44,
                    'price_yes_bid': 0.40,
                    'fair_prob': 0.50,
                    'edge_vs_mid': 0.08,
                    'edge_vs_ask': 0.06,
                    'candidate_rank': 1,
                    'candidate_bucket': 'priority',
                },
                {
                    'market_ticker': 'HIST_LAX_70_0321',
                    'market_title': 'Los Angeles watch setup',
                    'city_id': 'lax',
                    'minutes_to_close': 480,
                    'price_yes_mid': 0.36,
                    'price_yes_ask': 0.37,
                    'price_yes_bid': 0.35,
                    'fair_prob': 0.41,
                    'edge_vs_mid': 0.05,
                    'edge_vs_ask': 0.04,
                    'candidate_rank': 2,
                    'candidate_bucket': 'watch',
                },
            ],
            paper_limit_price=0.44,
        )


class HistorySnapshotTests(HistoricalSeedMixin, unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / 'history_snapshot.duckdb'
        bootstrap(db_path=self.db_path)
        load_all_registries(db_path=self.db_path)
        self.seed_history()

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_history_snapshot_summarizes_mixed_open_and_closed_learning_data(self):
        snapshot = get_history_snapshot(db_path=self.db_path)

        self.assertTrue(snapshot['has_history'])
        self.assertTrue(snapshot['has_closed_history'])
        self.assertEqual(snapshot['metrics']['strategy_session_count'], 4)
        self.assertEqual(snapshot['metrics']['open_paper_bet_count'], 1)
        self.assertEqual(snapshot['metrics']['closed_paper_bet_count'], 2)
        self.assertAlmostEqual(snapshot['metrics']['cumulative_realized_pnl'], -0.1)
        self.assertAlmostEqual(snapshot['metrics']['win_rate'], 0.5)
        self.assertEqual(len(snapshot['timeline']['daily']), 2)
        self.assertEqual(len(snapshot['timeline']['weekly']), 2)

        variant_rows = {row['group_key']: row for row in snapshot['grouped_performance']['variant_rows']}
        self.assertIn('baseline', variant_rows)
        self.assertIn('baseline-v2', variant_rows)
        self.assertIn('baseline-v3', variant_rows)
        self.assertEqual(variant_rows['baseline']['closed_count'], 1)
        self.assertEqual(variant_rows['baseline-v2']['closed_count'], 1)

        scenario_rows = {row['group_key']: row for row in snapshot['grouped_performance']['scenario_rows']}
        self.assertIn('cold_snap', scenario_rows)
        self.assertIn('warm_shift', scenario_rows)
        self.assertIn('carry_forward', scenario_rows)

        city_rows = {row['group_key']: row for row in snapshot['grouped_performance']['city_rows']}
        self.assertIn('nyc', city_rows)
        self.assertIn('chi', city_rows)
        self.assertIn('lax', city_rows)

        candidate_rows = {row['group_key']: row for row in snapshot['grouped_performance']['candidate_rows']}
        self.assertEqual(candidate_rows['priority']['board_count'], 4)
        self.assertEqual(candidate_rows['watch']['board_count'], 3)
        self.assertEqual(candidate_rows['pass']['board_count'], 1)

        approval_rows = {row['group_key']: row for row in snapshot['grouped_performance']['approval_rows']}
        self.assertEqual(approval_rows['approved']['proposal_count'], 2)
        self.assertEqual(approval_rows['adjustments_requested']['proposal_count'], 1)
        self.assertEqual(approval_rows['rejected']['proposal_count'], 1)

        time_rows = {row['group_key']: row for row in snapshot['grouped_performance']['time_bucket_rows']}
        self.assertIn('<2h', time_rows)
        self.assertIn('2-6h', time_rows)
        self.assertIn('6-12h', time_rows)
        self.assertIn('12h+', time_rows)

        threshold_rows = {row['group_key']: row for row in snapshot['recommendation_quality']['threshold_rows']}
        self.assertIn('50-54F', threshold_rows)
        self.assertIn('60-64F', threshold_rows)

        recurring = snapshot['learning_review']['recurring_lessons']
        self.assertTrue(any(row['theme'] == 'Wait for the better entry after the full-board scan.' for row in recurring))
        self.assertTrue(any(row['approval_outcome'] == 'adjustments_requested' for row in snapshot['learning_review']['review_change_log']))


class EmptyHistorySnapshotTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / 'history_empty.duckdb'
        bootstrap(db_path=self.db_path)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_history_snapshot_handles_empty_state_without_fake_labels(self):
        snapshot = get_history_snapshot(db_path=self.db_path)

        self.assertFalse(snapshot['has_history'])
        self.assertFalse(snapshot['has_closed_history'])
        self.assertEqual(snapshot['metrics']['strategy_session_count'], 0)
        self.assertEqual(snapshot['metrics']['closed_paper_bet_count'], 0)
        self.assertEqual(snapshot['timeline']['daily'], [])
        self.assertEqual(snapshot['timeline']['weekly'], [])
        self.assertEqual(snapshot['grouped_performance']['variant_rows'], [])
        self.assertEqual(snapshot['learning_review']['recent_settled_bets'], [])


@unittest.skipUnless(TestClient is not None and create_app is not None, 'fastapi test dependencies are not installed')
class HistoryPageTests(HistoricalSeedMixin, unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / 'history_page.duckdb'
        bootstrap(db_path=self.db_path)
        load_all_registries(db_path=self.db_path)
        self.seed_history()
        self.client = TestClient(create_app(db_path=self.db_path))

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_history_page_renders_grouped_learning_surfaces(self):
        response = self.client.get('/history')
        self.assertEqual(response.status_code, 200)
        self.assertIn('Performance review and strategy learning', response.text)
        self.assertIn('Closed paper performance by day', response.text)
        self.assertIn('Recent strategy outcomes', response.text)
        self.assertIn('What changed after review', response.text)
        self.assertIn('Wait for the better entry after the full-board scan.', response.text)
        self.assertIn('baseline-v2', response.text)
        self.assertIn('LAX', response.text)


@unittest.skipUnless(TestClient is not None and create_app is not None, 'fastapi test dependencies are not installed')
class EmptyHistoryPageTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / 'history_page_empty.duckdb'
        bootstrap(db_path=self.db_path)
        self.client = TestClient(create_app(db_path=self.db_path))

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_history_page_renders_empty_state_cleanly(self):
        response = self.client.get('/history')
        self.assertEqual(response.status_code, 200)
        self.assertIn('Historical review will build as sessions and paper bets accumulate', response.text)
        self.assertIn('No closed paper bets yet, so there is no daily performance timeline to review.', response.text)
        self.assertIn('No strategy sessions recorded yet.', response.text)


if __name__ == '__main__':
    unittest.main()
