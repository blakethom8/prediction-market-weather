import tempfile
import unittest
from datetime import UTC, date, datetime
from pathlib import Path

from weatherlab.build.bootstrap import bootstrap
from weatherlab.db import connect
from weatherlab.ingest.settlement_observations import ingest_settlement_observation


class TruthProvenanceTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / 'truth_provenance.duckdb'
        bootstrap(db_path=self.db_path)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_settlement_source_comparison_view(self):
        ingest_settlement_observation(
            source='nws-cli',
            station_id='KNYC',
            city_id='nyc',
            market_date_local=date(2026, 3, 18),
            observed_high_temp_f=73.0,
            report_published_at_utc=datetime(2026, 3, 19, 10, 0, tzinfo=UTC),
            db_path=self.db_path,
        )
        ingest_settlement_observation(
            source='kalshi-implied',
            station_id='KNYC',
            city_id='nyc',
            market_date_local=date(2026, 3, 18),
            observed_high_temp_f=72.5,
            report_published_at_utc=datetime(2026, 3, 19, 11, 0, tzinfo=UTC),
            db_path=self.db_path,
        )

        con = connect(db_path=self.db_path)
        try:
            row = con.execute(
                '''
                select city_id, official_high_temp_f, implied_high_temp_f, implied_minus_official_high_f
                from features.v_settlement_source_comparison
                where city_id = 'nyc' and market_date_local = date '2026-03-18'
                '''
            ).fetchone()
        finally:
            con.close()

        self.assertEqual(row[0], 'nyc')
        self.assertEqual(row[1], 73.0)
        self.assertEqual(row[2], 72.5)
        self.assertAlmostEqual(row[3], -0.5)


if __name__ == '__main__':
    unittest.main()
