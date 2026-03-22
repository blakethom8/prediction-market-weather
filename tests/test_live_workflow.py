import json
import tempfile
import unittest
from datetime import UTC, date, datetime
from pathlib import Path

from weatherlab.build.bootstrap import bootstrap
from weatherlab.build.registry_loader import load_all_registries
from weatherlab.build.training_rows import materialize_training_rows
from weatherlab.ingest.contracts import ingest_contract
from weatherlab.ingest.market_snapshots import ingest_market_snapshot
from weatherlab.ingest.open_meteo import ingest_open_meteo_daily_payload
from weatherlab.ingest.settlement_observations import ingest_settlement_observation
from weatherlab.live.workflow import generate_daily_strategy_package


class LiveWorkflowTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / 'live_workflow.duckdb'
        self.artifacts_dir = Path(self.tmpdir.name) / 'artifacts'
        bootstrap(db_path=self.db_path)
        load_all_registries(db_path=self.db_path)

    def tearDown(self):
        self.tmpdir.cleanup()

    def _seed_market(self):
        ingest_contract(
            market_ticker='TEST_NYC_54',
            event_ticker='TEST_EVENT',
            title='Will the high temp in NYC be above 54 on Mar 23, 2026?',
            close_time_utc=datetime(2026, 3, 23, 16, 0, tzinfo=UTC),
            settlement_time_utc=datetime(2026, 3, 24, 12, 0, tzinfo=UTC),
            db_path=self.db_path,
        )
        ingest_open_meteo_daily_payload(
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

    def test_generate_daily_strategy_package(self):
        self._seed_market()
        result = generate_daily_strategy_package(
            strategy_date_local=date(2026, 3, 23),
            thesis='Use the daily board to compare paper bets before approving any one trade.',
            focus_cities=['nyc'],
            artifacts_dir=self.artifacts_dir,
            db_path=self.db_path,
        )

        self.assertEqual(result['board_count'], 1)
        self.assertEqual(result['summary']['board_size'], 1)
        self.assertTrue(Path(result['json_path']).exists())
        self.assertTrue(Path(result['markdown_path']).exists())
        self.assertTrue(Path(result['html_path']).exists())

        payload = json.loads(Path(result['json_path']).read_text())
        self.assertEqual(payload['summary']['focus_cities'], ['nyc'])
        self.assertEqual(payload['board_rows'][0]['market_ticker'], 'TEST_NYC_54')
        markdown = Path(result['markdown_path']).read_text()
        self.assertIn('Daily Strategy Summary', markdown)
        self.assertIn('TEST_NYC_54', markdown)


if __name__ == '__main__':
    unittest.main()
