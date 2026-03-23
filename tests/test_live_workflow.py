import json
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
from weatherlab.live.workflow import apply_strategy_review, generate_daily_strategy_package


class LiveWorkflowTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / 'live_workflow.duckdb'
        self.artifacts_dir = Path(self.tmpdir.name) / 'artifacts'
        bootstrap(db_path=self.db_path)
        load_all_registries(db_path=self.db_path)

    def tearDown(self):
        self.tmpdir.cleanup()

    def _seed_market(
        self,
        *,
        market_ticker: str,
        event_ticker: str,
        title: str,
        city_id: str,
        station_id: str,
        threshold_f: float,
        forecast_high_f: float,
        ask: float,
        bid: float,
        observed_high_f: float,
    ) -> None:
        ingest_contract(
            market_ticker=market_ticker,
            event_ticker=event_ticker,
            title=title,
            close_time_utc=datetime(2026, 3, 23, 16, 0, tzinfo=UTC),
            settlement_time_utc=datetime(2026, 3, 24, 12, 0, tzinfo=UTC),
            db_path=self.db_path,
        )
        ingest_open_meteo_daily_payload(
            payload={
                'daily': {
                    'time': ['2026-03-23'],
                    'temperature_2m_max': [forecast_high_f],
                    'temperature_2m_min': [43.0],
                    'precipitation_probability_max': [20],
                }
            },
            city_id=city_id,
            target_date_local=date(2026, 3, 23),
            thresholds=[threshold_f],
            fetched_at_utc=datetime(2026, 3, 23, 9, 0, tzinfo=UTC),
            db_path=self.db_path,
        )
        ingest_market_snapshot(
            market_ticker=market_ticker,
            ts_utc=datetime(2026, 3, 23, 10, 0, tzinfo=UTC),
            price_yes_bid=bid,
            price_yes_ask=ask,
            price_no_bid=0.54,
            price_no_ask=0.58,
            last_price=(ask + bid) / 2,
            volume=100,
            open_interest=25,
            minutes_to_close=360,
            db_path=self.db_path,
        )
        ingest_settlement_observation(
            source='nws-cli',
            station_id=station_id,
            city_id=city_id,
            market_date_local=date(2026, 3, 23),
            observed_high_temp_f=observed_high_f,
            report_published_at_utc=datetime(2026, 3, 24, 10, 0, tzinfo=UTC),
            db_path=self.db_path,
        )

    def _seed_board(self):
        self._seed_market(
            market_ticker='TEST_NYC_54',
            event_ticker='TEST_EVENT_NYC',
            title='Will the high temp in NYC be above 54 on Mar 23, 2026?',
            city_id='nyc',
            station_id='KNYC',
            threshold_f=54.0,
            forecast_high_f=56.0,
            ask=0.45,
            bid=0.42,
            observed_high_f=57.0,
        )
        self._seed_market(
            market_ticker='TEST_CHI_58',
            event_ticker='TEST_EVENT_CHI',
            title='Will the high temp in Chicago be above 58 on Mar 23, 2026?',
            city_id='chi',
            station_id='KORD',
            threshold_f=58.0,
            forecast_high_f=63.0,
            ask=0.32,
            bid=0.30,
            observed_high_f=61.0,
        )
        materialize_training_rows(db_path=self.db_path)

    def test_generate_daily_strategy_package_scans_broad_board_and_creates_proposals(self):
        self._seed_board()
        result = generate_daily_strategy_package(
            strategy_date_local=date(2026, 3, 23),
            thesis='Scan the broad live board before selecting paper bets.',
            research_focus_cities=['nyc'],
            strategy_variant='baseline-v2',
            scenario_label='sandbox-compare',
            artifacts_dir=self.artifacts_dir,
            db_path=self.db_path,
        )

        self.assertEqual(result['board_count'], 2)
        self.assertEqual(result['summary']['board_size'], 2)
        self.assertEqual(result['summary']['research_focus_cities'], ['nyc'])
        self.assertEqual(result['summary']['board_scope'], 'all_markets')
        self.assertEqual(result['summary']['board_city_count'], 2)
        self.assertEqual(result['summary']['board_city_ids'], ['chi', 'nyc'])
        self.assertGreaterEqual(result['proposal_count'], 1)
        self.assertTrue(Path(result['json_path']).exists())
        self.assertTrue(Path(result['markdown_path']).exists())
        self.assertTrue(Path(result['html_path']).exists())

        payload = json.loads(Path(result['json_path']).read_text())
        self.assertEqual(payload['summary']['research_focus_cities'], ['nyc'])
        self.assertEqual(len(payload['board_rows']), 2)
        self.assertEqual(payload['proposal_rows'][0]['strategy_variant'], 'baseline-v2')
        markdown = Path(result['markdown_path']).read_text()
        self.assertIn('Research focus cities: nyc', markdown)
        self.assertIn('Board scope: All available markets', markdown)
        self.assertIn('TEST_CHI_58', markdown)
        self.assertIn('TEST_NYC_54', markdown)

        con = connect(db_path=self.db_path)
        try:
            proposal_rows = con.execute(
                '''
                select count(*), min(strategy_variant), min(scenario_label)
                from ops.bet_proposals
                where strategy_id = ?
                ''',
                [result['strategy_id']],
            ).fetchone()
        finally:
            con.close()

        self.assertEqual(proposal_rows[0], result['proposal_count'])
        self.assertEqual(proposal_rows[1], 'baseline-v2')
        self.assertEqual(proposal_rows[2], 'sandbox-compare')

    def test_apply_strategy_review_updates_approval_and_proposal_history(self):
        self._seed_board()
        result = generate_daily_strategy_package(
            strategy_date_local=date(2026, 3, 23),
            thesis='Scan the broad live board before selecting paper bets.',
            research_focus_cities=['nyc'],
            artifacts_dir=self.artifacts_dir,
            db_path=self.db_path,
        )
        apply_strategy_review(
            strategy_id=result['strategy_id'],
            decision='adjust',
            notes={'reason': 'Reduce exposure and revisit near close.'},
            db_path=self.db_path,
        )

        con = connect(db_path=self.db_path)
        try:
            strategy_row = con.execute(
                '''
                select approval_status, approved_at_utc, last_reviewed_at_utc, approval_notes_json
                from ops.strategy_sessions
                where strategy_id = ?
                ''',
                [result['strategy_id']],
            ).fetchone()
            review_event = con.execute(
                '''
                select decision, resulting_approval_status, notes_json
                from ops.strategy_review_events
                where strategy_id = ?
                ''',
                [result['strategy_id']],
            ).fetchone()
            proposal_statuses = con.execute(
                '''
                select distinct proposal_status
                from ops.bet_proposals
                where strategy_id = ?
                ''',
                [result['strategy_id']],
            ).fetchall()
            proposal_events = con.execute(
                '''
                select count(*)
                from ops.bet_proposal_events
                where strategy_id = ?
                  and decision = 'adjust'
                ''',
                [result['strategy_id']],
            ).fetchone()
        finally:
            con.close()

        self.assertEqual(strategy_row[0], 'adjustments_requested')
        self.assertIsNone(strategy_row[1])
        self.assertIsNotNone(strategy_row[2])
        self.assertIn('Reduce exposure', strategy_row[3])
        self.assertEqual(review_event[0], 'adjust')
        self.assertEqual(review_event[1], 'adjustments_requested')
        self.assertIn('Reduce exposure', review_event[2])
        self.assertEqual({row[0] for row in proposal_statuses}, {'adjustments_requested'})
        self.assertEqual(proposal_events[0], result['proposal_count'])


if __name__ == '__main__':
    unittest.main()
