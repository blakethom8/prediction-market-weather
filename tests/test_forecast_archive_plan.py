import os
import unittest
from unittest.mock import patch

from weatherlab.forecast_archive_plan import (
    ARCHIVE_SOURCE_CANDIDATES,
    get_focus_city_archive_plan,
    get_focus_city_ids,
)


class ForecastArchivePlanTests(unittest.TestCase):
    def test_default_focus_cities_are_nyc_and_chi(self):
        with patch.dict(os.environ, {}, clear=True):
            # Import-time settings may already be loaded, so assert through fallback behavior.
            self.assertEqual(get_focus_city_ids(), ('nyc', 'chi'))

    def test_focus_city_archive_plan_uses_primary_and_fallback_sources(self):
        plan = get_focus_city_archive_plan()
        self.assertEqual(plan['nyc']['primary_source'], 'ndfd-archive')
        self.assertEqual(plan['chi']['fallback_source'], 'iem-nws-text')
        self.assertEqual(plan['nyc']['proxy_source'], 'open-meteo-archive')

    def test_archive_source_candidates_include_proxy_marker(self):
        source_ids = {candidate.source_id: candidate.role for candidate in ARCHIVE_SOURCE_CANDIDATES}
        self.assertEqual(source_ids['ndfd-archive'], 'primary')
        self.assertEqual(source_ids['iem-nws-text'], 'fallback')
        self.assertEqual(source_ids['open-meteo-archive'], 'proxy_only')


if __name__ == '__main__':
    unittest.main()
