import tempfile
import unittest
from datetime import UTC, date, datetime
from pathlib import Path

from weatherlab.build.bootstrap import bootstrap
from weatherlab.build.registry_loader import load_all_registries
from weatherlab.db import connect
from weatherlab.ingest.contracts import ingest_contract
from weatherlab.ingest.market_snapshots import ingest_market_snapshot
from weatherlab.ingest.open_meteo import ingest_open_meteo_daily_payload
from weatherlab.ingest.settlement_observations import ingest_settlement_observation


class CityDiagnosticsTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / 'city_diagnostics.duckdb'
        bootstrap(db_path=self.db_path)
        load_all_registries(db_path=self.db_path)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_city_source_coverage_view(self):
        ingest_contract(
            market_ticker='TEST_NYC_70',
            event_ticker='TEST_EVENT',
            title='Will the high temp in NYC be above 70 on Mar 18, 2026?',
            close_time_utc=datetime(2026, 3, 18, 16, 0, tzinfo=UTC),
            settlement_time_utc=datetime(2026, 3, 19, 12, 0, tzinfo=UTC),
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
            fetched_at_utc=datetime(2026, 3, 18, 8, 0, tzinfo=UTC),
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

        con = connect(db_path=self.db_path)
        try:
            row = con.execute(
                """
                select city_id, contract_count, market_snapshot_count, open_meteo_live_count,
                       official_settlement_count, readiness_band
                from features.v_city_source_coverage
                where city_id = 'nyc'
                """
            ).fetchone()
        finally:
            con.close()

        self.assertEqual(row[0], 'nyc')
        self.assertEqual(row[1], 1)
        self.assertEqual(row[2], 1)
        self.assertEqual(row[3], 1)
        self.assertEqual(row[4], 1)
        self.assertEqual(row[5], 'research_ready')


if __name__ == '__main__':
    unittest.main()
