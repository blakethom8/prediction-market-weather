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

    def test_weather_city_registry_uses_airport_coordinates(self):
        load_all_registries(db_path=self.db_path)

        expected_coords = {
            'dc': (38.8521, -77.0377),
            'mia': (25.7959, -80.2870),
            'bos': (42.3601, -71.0105),
            'phl': (39.8721, -75.2411),
            'lax': (33.9425, -118.4081),
            'chi': (41.7868, -87.7522),
            'den': (39.8561, -104.6737),
        }

        con = connect(db_path=self.db_path)
        try:
            rows = con.execute(
                '''
                select city_id, lat, lon
                from core.cities
                where city_id in ('dc', 'mia', 'bos', 'phl', 'lax', 'chi', 'den')
                '''
            ).fetchall()
        finally:
            con.close()

        self.assertEqual(len(rows), len(expected_coords))
        for city_id, lat, lon in rows:
            expected_lat, expected_lon = expected_coords[city_id]
            self.assertAlmostEqual(lat, expected_lat, places=4)
            self.assertAlmostEqual(lon, expected_lon, places=4)


if __name__ == '__main__':
    unittest.main()
