import tempfile
import unittest
from datetime import UTC, date, datetime
from pathlib import Path

from weatherlab.build.bootstrap import bootstrap
from weatherlab.db import connect
from weatherlab.ingest.open_meteo import (
    build_threshold_distribution,
    ingest_open_meteo_daily_payload,
    parse_open_meteo_daily_payload,
)


class OpenMeteoTests(unittest.TestCase):
    def setUp(self):
        self.payload = {
            'daily': {
                'time': ['2026-03-18', '2026-03-19'],
                'temperature_2m_max': [72.0, 68.0],
                'temperature_2m_min': [55.0, 52.0],
                'precipitation_probability_max': [30, 10],
            }
        }

    def test_build_threshold_distribution_is_monotone(self):
        distribution = build_threshold_distribution(point_temp_f=72.0, thresholds=[68, 70, 72, 74])
        self.assertGreater(distribution[68.0], distribution[70.0])
        self.assertGreater(distribution[70.0], distribution[72.0])
        self.assertGreater(distribution[72.0], distribution[74.0])

    def test_parse_open_meteo_daily_payload(self):
        parsed = parse_open_meteo_daily_payload(
            payload=self.payload,
            city_id='nyc',
            target_date_local=date(2026, 3, 18),
            thresholds=[70.0, 75.0],
            fetched_at_utc=datetime(2026, 3, 18, 8, 0, tzinfo=UTC),
        )
        self.assertEqual(parsed['city_id'], 'nyc')
        self.assertEqual(parsed['pred_high_temp_f'], 72.0)
        self.assertEqual(parsed['pred_low_temp_f'], 55.0)
        self.assertAlmostEqual(parsed['pred_precip_prob'], 0.30)
        self.assertIn(70.0, parsed['distribution'])

    def test_ingest_open_meteo_daily_payload(self):
        tmpdir = tempfile.TemporaryDirectory()
        db_path = Path(tmpdir.name) / 'open_meteo_test.duckdb'
        try:
            bootstrap(db_path=db_path)
            forecast_id = ingest_open_meteo_daily_payload(
                payload=self.payload,
                city_id='nyc',
                target_date_local=date(2026, 3, 18),
                thresholds=[70.0],
                fetched_at_utc=datetime(2026, 3, 18, 8, 0, tzinfo=UTC),
                db_path=db_path,
            )
            con = connect(db_path=db_path)
            try:
                row = con.execute(
                    'select city_id, pred_high_temp_f from core.forecast_snapshots where forecast_snapshot_id = ?',
                    [forecast_id],
                ).fetchone()
                prob = con.execute(
                    'select prob_ge_threshold from core.forecast_distributions where forecast_snapshot_id = ? and threshold_f = 70',
                    [forecast_id],
                ).fetchone()[0]
            finally:
                con.close()
        finally:
            tmpdir.cleanup()

        self.assertEqual(row[0], 'nyc')
        self.assertEqual(row[1], 72.0)
        self.assertGreater(prob, 0.0)
        self.assertLess(prob, 1.0)


if __name__ == '__main__':
    unittest.main()
