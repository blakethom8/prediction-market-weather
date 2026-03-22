import tempfile
import unittest
from datetime import UTC, date, datetime
from pathlib import Path
from unittest.mock import patch

from weatherlab.build.bootstrap import bootstrap
from weatherlab.build.registry_loader import load_all_registries
from weatherlab.db import connect
from weatherlab.ingest.contracts import ingest_contract
from weatherlab.ingest.historical_forecasts import backfill_historical_forecasts


class HistoricalForecastBackfillTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / 'historical_forecasts.duckdb'
        bootstrap(db_path=self.db_path)
        load_all_registries(db_path=self.db_path)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_backfill_can_be_limited_to_focus_cities(self):
        ingest_contract(
            market_ticker='TEST_NYC_70',
            event_ticker='TEST_EVENT_NYC',
            title='Will the high temp in NYC be above 70 on Mar 18, 2026?',
            close_time_utc=datetime(2026, 3, 18, 16, 0, tzinfo=UTC),
            settlement_time_utc=datetime(2026, 3, 19, 12, 0, tzinfo=UTC),
            db_path=self.db_path,
        )
        ingest_contract(
            market_ticker='TEST_MIA_80',
            event_ticker='TEST_EVENT_MIA',
            title='Will the high temp in Miami be above 80 on Mar 18, 2026?',
            close_time_utc=datetime(2026, 3, 18, 16, 0, tzinfo=UTC),
            settlement_time_utc=datetime(2026, 3, 19, 12, 0, tzinfo=UTC),
            db_path=self.db_path,
        )

        def fake_fetch_archive(**kwargs):
            return {
                'daily': {
                    'time': ['2026-03-18'],
                    'temperature_2m_max': [72.0],
                    'temperature_2m_min': [55.0],
                    'precipitation_sum': [0.0],
                }
            }

        with patch('weatherlab.ingest.historical_forecasts._fetch_archive', side_effect=fake_fetch_archive) as mocked:
            inserted = backfill_historical_forecasts(db_path=self.db_path, city_ids=['nyc'])
            self.assertEqual(inserted, 1)
            self.assertEqual(mocked.call_count, 1)

        con = connect(db_path=self.db_path)
        try:
            rows = con.execute(
                'select city_id, source from core.forecast_snapshots order by city_id'
            ).fetchall()
        finally:
            con.close()

        self.assertEqual(rows, [('nyc', 'open-meteo-archive')])


if __name__ == '__main__':
    unittest.main()
