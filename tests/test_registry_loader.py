import tempfile
import unittest
from pathlib import Path

from weatherlab.build.bootstrap import bootstrap
from weatherlab.build.registry_loader import load_all_registries
from weatherlab.db import connect


class RegistryLoaderTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / 'registry_test.duckdb'
        bootstrap(db_path=self.db_path)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_load_all_registries(self):
        counts = load_all_registries(db_path=self.db_path)
        self.assertGreaterEqual(counts['cities'], 5)
        self.assertGreaterEqual(counts['stations'], 5)

        con = connect(db_path=self.db_path)
        try:
            city_count = con.execute('select count(*) from core.cities').fetchone()[0]
            station_count = con.execute('select count(*) from core.weather_stations').fetchone()[0]
            primary_station = con.execute(
                "select primary_station_id from core.cities where city_id = 'nyc'"
            ).fetchone()[0]
        finally:
            con.close()

        self.assertEqual(city_count, counts['cities'])
        self.assertEqual(station_count, counts['stations'])
        self.assertEqual(primary_station, 'KNYC')


if __name__ == '__main__':
    unittest.main()
