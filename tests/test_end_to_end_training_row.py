import tempfile
import unittest
from datetime import date, datetime, UTC
from pathlib import Path

from weatherlab.build.bootstrap import bootstrap
from weatherlab.build.registry_loader import load_all_registries
from weatherlab.build.training_rows import materialize_training_rows
from weatherlab.db import connect
from weatherlab.ingest.contracts import ingest_contract
from weatherlab.ingest.market_snapshots import ingest_market_snapshot
from weatherlab.ingest.open_meteo import (
    build_threshold_distribution,
    ingest_open_meteo_daily_payload,
)
from weatherlab.ingest.settlement_observations import ingest_settlement_observation


class EndToEndTrainingRowTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / 'pipeline_test.duckdb'
        bootstrap(db_path=self.db_path)
        load_all_registries(db_path=self.db_path)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_end_to_end_training_row_materialization(self):
        ingest_contract(
            market_ticker='TEST_NYC_70',
            event_ticker='TEST_EVENT',
            title='Will the high temp in NYC be above 70 on Mar 18, 2026?',
            close_time_utc=datetime(2026, 3, 18, 16, 0, tzinfo=UTC),
            settlement_time_utc=datetime(2026, 3, 19, 12, 0, tzinfo=UTC),
            db_path=self.db_path,
        )
        forecast_id = ingest_open_meteo_daily_payload(
            payload={
                'daily': {
                    'time': ['2026-03-18'],
                    'temperature_2m_max': [72.0],
                    'temperature_2m_min': [55.0],
                    'precipitation_probability_max': [30],
                }
            },
            city_id='nyc',
            target_date_local=date(2026, 3, 18),
            thresholds=[70.0],
            fetched_at_utc=datetime(2026, 3, 18, 9, 5, tzinfo=UTC),
            sigma_f=2.77,
            db_path=self.db_path,
        )
        ingest_market_snapshot(
            market_ticker='TEST_NYC_70',
            ts_utc=datetime(2026, 3, 18, 10, 0, tzinfo=UTC),
            price_yes_bid=0.48,
            price_yes_ask=0.52,
            price_no_bid=0.46,
            price_no_ask=0.50,
            last_price=0.51,
            volume=100,
            open_interest=25,
            minutes_to_close=360,
            db_path=self.db_path,
        )
        ingest_settlement_observation(
            source='nws-cli',
            station_id='KNYC',
            city_id='nyc',
            market_date_local=date(2026, 3, 18),
            observed_high_temp_f=73.0,
            report_published_at_utc=datetime(2026, 3, 19, 10, 0, tzinfo=UTC),
            is_final=True,
            db_path=self.db_path,
        )

        inserted = materialize_training_rows(db_path=self.db_path)
        self.assertEqual(inserted, 1)

        con = connect(db_path=self.db_path)
        try:
            row = con.execute(
                '''
                select market_ticker, fair_prob, edge_vs_mid, y_resolve_yes, latest_forecast_snapshot_id
                from features.contract_training_rows
                where market_ticker = 'TEST_NYC_70'
                '''
            ).fetchone()
        finally:
            con.close()

        expected_prob = build_threshold_distribution(
            point_temp_f=72.0,
            thresholds=[70.0],
            sigma_f=2.77,
        )[70.0]

        self.assertEqual(row[0], 'TEST_NYC_70')
        self.assertAlmostEqual(row[1], expected_prob, places=4)
        self.assertAlmostEqual(row[2], expected_prob - 0.50, places=4)
        self.assertEqual(row[3], 1)
        self.assertEqual(row[4], forecast_id)


if __name__ == '__main__':
    unittest.main()
