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


class TrainingViewSettlementPreferenceTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / 'settlement_pref.duckdb'
        bootstrap(db_path=self.db_path)
        load_all_registries(db_path=self.db_path)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_training_rows_prefer_official_truth_over_implied(self):
        ingest_contract(
            market_ticker='TEST_NYC_70',
            event_ticker='TEST_EVENT',
            title='Will the high temp in NYC be above 70 on Mar 18, 2026?',
            close_time_utc=datetime(2026, 3, 18, 16, 0, tzinfo=UTC),
            settlement_time_utc=datetime(2026, 3, 19, 12, 0, tzinfo=UTC),
            db_path=self.db_path,
        )
        ingest_open_meteo_daily_payload(
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
            db_path=self.db_path,
        )
        ingest_market_snapshot(
            market_ticker='TEST_NYC_70',
            ts_utc=datetime(2026, 3, 18, 10, 0, tzinfo=UTC),
            price_yes_bid=0.48,
            price_yes_ask=0.52,
            last_price=0.51,
            volume=100,
            open_interest=25,
            minutes_to_close=360,
            db_path=self.db_path,
        )
        ingest_settlement_observation(
            source='kalshi-implied',
            station_id='KNYC',
            city_id='nyc',
            market_date_local=date(2026, 3, 18),
            observed_high_temp_f=68.0,
            report_published_at_utc=datetime(2026, 3, 19, 9, 0, tzinfo=UTC),
            db_path=self.db_path,
        )
        ingest_settlement_observation(
            source='nws-cli',
            station_id='KNYC',
            city_id='nyc',
            market_date_local=date(2026, 3, 18),
            observed_high_temp_f=73.0,
            report_published_at_utc=datetime(2026, 3, 19, 10, 0, tzinfo=UTC),
            db_path=self.db_path,
        )

        inserted = materialize_training_rows(db_path=self.db_path)
        self.assertEqual(inserted, 1)

        con = connect(db_path=self.db_path)
        try:
            row = con.execute(
                "select y_resolve_yes from features.contract_training_rows where market_ticker = 'TEST_NYC_70'"
            ).fetchone()
        finally:
            con.close()

        self.assertEqual(row[0], 1)


if __name__ == '__main__':
    unittest.main()
