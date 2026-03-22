import tempfile
import unittest
from datetime import UTC, date, datetime
from pathlib import Path
from unittest.mock import patch

from weatherlab.build.bootstrap import bootstrap
from weatherlab.build.registry_loader import load_all_registries
from weatherlab.db import connect
from weatherlab.ingest.archived_nws_forecasts import (
    backfill_archived_nws_zone_forecasts,
    fetch_archived_nws_text_products,
    parse_archived_nws_zone_forecast,
)
from weatherlab.ingest.contracts import ingest_contract

NYC_FIXTURE = Path(__file__).parent / 'fixtures' / 'iem_zfp_nyc_2026-03-19.txt'
CHI_FIXTURE = Path(__file__).parent / 'fixtures' / 'iem_zfp_chi_2026-03-19.txt'


class ArchivedNwsForecastTests(unittest.TestCase):
    def test_parse_archived_nyc_forecast(self):
        parsed = parse_archived_nws_zone_forecast(
            text=NYC_FIXTURE.read_text(),
            city_id='nyc',
            target_date_local=date(2026, 3, 20),
            thresholds=[55.0, 60.0],
        )
        self.assertEqual(parsed['source'], 'iem-zfp')
        self.assertEqual(parsed['city_id'], 'nyc')
        self.assertEqual(parsed['target_date_local'], date(2026, 3, 20))
        self.assertAlmostEqual(parsed['pred_high_temp_f'], 58.0)
        self.assertIn('Partly sunny', parsed['summary_text'])
        self.assertEqual(parsed['raw_ref'], 'ZFPOKX')

    def test_parse_archived_chicago_forecast(self):
        parsed = parse_archived_nws_zone_forecast(
            text=CHI_FIXTURE.read_text(),
            city_id='chi',
            target_date_local=date(2026, 3, 20),
            thresholds=[58.0, 62.0],
        )
        self.assertAlmostEqual(parsed['pred_high_temp_f'], 62.0)
        self.assertIn('Mostly cloudy', parsed['summary_text'])
        self.assertEqual(parsed['raw_ref'], 'ZFPLOT')

    def test_fetch_archived_products_splits_multiple_messages(self):
        joined = NYC_FIXTURE.read_text() + CHI_FIXTURE.read_text()

        class FakeResponse:
            def __init__(self, text):
                self.text = text

            def raise_for_status(self):
                return None

        with patch('weatherlab.ingest.archived_nws_forecasts.requests.get', return_value=FakeResponse(joined)):
            products = fetch_archived_nws_text_products(
                pil='ZFPOKX',
                start_utc=datetime(2026, 3, 19, 0, 0, tzinfo=UTC),
                end_utc=datetime(2026, 3, 20, 0, 0, tzinfo=UTC),
            )
        self.assertEqual(len(products), 2)

    def test_backfill_archived_zone_forecasts(self):
        tmpdir = tempfile.TemporaryDirectory()
        db_path = Path(tmpdir.name) / 'archived_forecasts.duckdb'
        try:
            bootstrap(db_path=db_path)
            load_all_registries(db_path=db_path)
            ingest_contract(
                market_ticker='TEST_NYC_60',
                event_ticker='TEST_EVENT_NYC',
                title='Will the high temp in NYC be above 60 on Mar 20, 2026?',
                close_time_utc=datetime(2026, 3, 20, 16, 0, tzinfo=UTC),
                settlement_time_utc=datetime(2026, 3, 21, 12, 0, tzinfo=UTC),
                db_path=db_path,
            )
            with patch(
                'weatherlab.ingest.archived_nws_forecasts.fetch_archived_nws_text_products',
                return_value=[NYC_FIXTURE.read_text()],
            ):
                inserted = backfill_archived_nws_zone_forecasts(db_path=db_path, city_ids=['nyc'])

            con = connect(db_path=db_path)
            try:
                row = con.execute(
                    "select source, city_id, pred_high_temp_f from core.forecast_snapshots where source = 'iem-zfp'"
                ).fetchone()
            finally:
                con.close()
        finally:
            tmpdir.cleanup()

        self.assertEqual(inserted, 1)
        self.assertEqual(row[0], 'iem-zfp')
        self.assertEqual(row[1], 'nyc')
        self.assertAlmostEqual(row[2], 58.0)


if __name__ == '__main__':
    unittest.main()
