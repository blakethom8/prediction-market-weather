import tempfile
import unittest
from pathlib import Path

from weatherlab.build.bootstrap import bootstrap
from weatherlab.db import connect
from weatherlab.ingest.nws_cli import ingest_nws_cli_text, parse_nws_cli_text

FIXTURE = Path(__file__).parent / 'fixtures' / 'nws_cli_nyc_2026-03-18.txt'


class NwsCliTests(unittest.TestCase):
    def test_parse_nws_cli_text(self):
        parsed = parse_nws_cli_text(FIXTURE.read_text())
        self.assertEqual(str(parsed['market_date_local']), '2026-03-18')
        self.assertEqual(parsed['observed_high_temp_f'], 73.0)
        self.assertEqual(parsed['observed_low_temp_f'], 55.0)
        self.assertEqual(parsed['observed_precip_in'], 0.0)

    def test_ingest_nws_cli_text(self):
        tmpdir = tempfile.TemporaryDirectory()
        db_path = Path(tmpdir.name) / 'nws_cli_test.duckdb'
        try:
            bootstrap(db_path=db_path)
            settlement_id = ingest_nws_cli_text(
                text=FIXTURE.read_text(),
                station_id='KNYC',
                city_id='nyc',
                db_path=db_path,
            )
            con = connect(db_path=db_path)
            try:
                row = con.execute(
                    'select city_id, observed_high_temp_f, observed_low_temp_f from core.settlement_observations where settlement_id = ?',
                    [settlement_id],
                ).fetchone()
            finally:
                con.close()
        finally:
            tmpdir.cleanup()

        self.assertEqual(row[0], 'nyc')
        self.assertEqual(row[1], 73.0)
        self.assertEqual(row[2], 55.0)


if __name__ == '__main__':
    unittest.main()
