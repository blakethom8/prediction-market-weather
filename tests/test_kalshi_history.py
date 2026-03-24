import tempfile
import unittest
from pathlib import Path

import duckdb

from weatherlab.build.bootstrap import bootstrap
from weatherlab.ingest.kalshi_history import (
    ExtractionResult,
    _build_weather_filter_sql,
    extract_weather_history,
    is_weather_ticker,
)


class WeatherTickerFilterTests(unittest.TestCase):
    """Tests for the weather ticker identification logic."""

    def test_known_weather_prefixes_match(self):
        self.assertTrue(is_weather_ticker('HIGHNY-25MAR19-B45.5'))
        self.assertTrue(is_weather_ticker('KXHIGHCHI-25NOV17-T92'))
        self.assertTrue(is_weather_ticker('KXHIGHTATL-25NOV17-T72'))
        self.assertTrue(is_weather_ticker('KXCITIESWEATHER-25MAR19'))
        self.assertTrue(is_weather_ticker('RAINNYC-25MAR19'))
        self.assertTrue(is_weather_ticker('SNOWNYM-25JAN05'))
        self.assertTrue(is_weather_ticker('HURCAT-25SEP01'))
        self.assertTrue(is_weather_ticker('KXHMONTHRANGE-25MAR'))

    def test_non_weather_tickers_rejected(self):
        self.assertFalse(is_weather_ticker('INXD-25MAR19'))
        self.assertFalse(is_weather_ticker('KXPRESPARTY-28'))
        self.assertFalse(is_weather_ticker('NCAAM-25MAR19'))
        self.assertFalse(is_weather_ticker('CPI-25APR10'))

    def test_sql_filter_produces_valid_clause(self):
        sql = _build_weather_filter_sql('t.ticker')
        self.assertIn("t.ticker LIKE 'HIGHNY%'", sql)
        self.assertIn("t.ticker LIKE 'KXCITIESWEATHER%'", sql)
        # Should be valid SQL — test by running it
        con = duckdb.connect()
        replaced = sql.replace('t.ticker', "'HIGHNY-TEST'")
        con.execute(f"SELECT 1 WHERE {replaced}")
        con.close()


class ExtractionPipelineTests(unittest.TestCase):
    """Integration test: create fake parquet files, run extraction, verify."""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.tmp = Path(self.tmpdir.name)
        self.db_path = self.tmp / 'test.duckdb'
        bootstrap(db_path=self.db_path)
        self._create_test_parquets()

    def tearDown(self):
        self.tmpdir.cleanup()

    def _create_test_parquets(self):
        """Write small parquet files mimicking the Kalshi archive layout."""
        markets_dir = self.tmp / 'kalshi' / 'markets'
        trades_dir = self.tmp / 'kalshi' / 'trades'
        markets_dir.mkdir(parents=True)
        trades_dir.mkdir(parents=True)

        con = duckdb.connect()

        # Markets: 3 weather + 2 non-weather
        con.execute(f"""
            COPY (
                SELECT * FROM (VALUES
                    ('HIGHNY-25MAR19-B45.5', 'HIGHNY-25MAR19',
                     'Will the **high temp in NYC** be 45-46° on Mar 19, 2025?',
                     '45° to 46°', 'finalized', 'yes',
                     TIMESTAMP '2025-03-18 15:00:00', TIMESTAMP '2025-03-20 05:59:00',
                     TIMESTAMP '2025-03-18 10:00:00'),
                    ('KXHIGHCHI-25NOV17-T92', 'KXHIGHCHI-25NOV17',
                     'Will the **high temp in Chicago** be >92° on Nov 17, 2025?',
                     '93° or above', 'finalized', 'no',
                     TIMESTAMP '2025-11-16 15:00:00', TIMESTAMP '2025-11-18 05:59:00',
                     TIMESTAMP '2025-11-16 10:00:00'),
                    ('RAINNYC-25MAR19', 'RAINNYC-25MAR19',
                     'Will it rain in NYC on Mar 19?',
                     'Yes', 'finalized', 'no',
                     TIMESTAMP '2025-03-18 15:00:00', TIMESTAMP '2025-03-20 05:59:00',
                     TIMESTAMP '2025-03-18 10:00:00'),
                    ('INXD-25MAR19', 'INXD-25MAR19',
                     'Will the S&P 500 close above 5000?',
                     'Yes', 'active', NULL,
                     TIMESTAMP '2025-03-18 15:00:00', TIMESTAMP '2025-03-20 05:59:00',
                     TIMESTAMP '2025-03-18 10:00:00'),
                    ('CPI-25APR10', 'CPI-25APR10',
                     'Will CPI come in above 3%?',
                     'Yes', 'active', NULL,
                     TIMESTAMP '2025-04-09 15:00:00', TIMESTAMP '2025-04-11 05:59:00',
                     TIMESTAMP '2025-04-09 10:00:00')
                ) AS t(ticker, event_ticker, title, yes_sub_title, status, result,
                       open_time, close_time, _fetched_at)
            ) TO '{markets_dir / "markets_0_5.parquet"}' (FORMAT PARQUET)
        """)

        # Trades: 4 weather + 2 non-weather
        con.execute(f"""
            COPY (
                SELECT * FROM (VALUES
                    ('trade_1', 'HIGHNY-25MAR19-B45.5', 10, 45, 55, 'yes',
                     TIMESTAMP '2025-03-19 12:00:00', TIMESTAMP '2025-03-20 00:00:00'),
                    ('trade_2', 'HIGHNY-25MAR19-B45.5', 5, 47, 53, 'no',
                     TIMESTAMP '2025-03-19 13:00:00', TIMESTAMP '2025-03-20 00:00:00'),
                    ('trade_3', 'KXHIGHCHI-25NOV17-T92', 20, 8, 92, 'yes',
                     TIMESTAMP '2025-11-17 14:00:00', TIMESTAMP '2025-11-18 00:00:00'),
                    ('trade_4', 'RAINNYC-25MAR19', 3, 30, 70, 'yes',
                     TIMESTAMP '2025-03-19 11:00:00', TIMESTAMP '2025-03-20 00:00:00'),
                    ('trade_5', 'INXD-25MAR19', 50, 60, 40, 'yes',
                     TIMESTAMP '2025-03-19 15:00:00', TIMESTAMP '2025-03-20 00:00:00'),
                    ('trade_6', 'CPI-25APR10', 15, 35, 65, 'no',
                     TIMESTAMP '2025-04-10 09:00:00', TIMESTAMP '2025-04-11 00:00:00')
                ) AS t(trade_id, ticker, count, yes_price, no_price, taker_side,
                       created_time, _fetched_at)
            ) TO '{trades_dir / "trades_0_6.parquet"}' (FORMAT PARQUET)
        """)
        con.close()

    def test_extraction_loads_weather_only(self):
        result = extract_weather_history(
            kalshi_dir=self.tmp / 'kalshi',
            db_path=self.db_path,
        )

        self.assertIsInstance(result, ExtractionResult)
        self.assertEqual(result.markets_loaded, 3)
        self.assertEqual(result.trades_loaded, 4)
        self.assertEqual(result.weather_tickers_found, 3)

    def test_non_weather_excluded(self):
        extract_weather_history(
            kalshi_dir=self.tmp / 'kalshi',
            db_path=self.db_path,
        )
        con = duckdb.connect(str(self.db_path))
        try:
            # No S&P or CPI rows
            inxd = con.execute(
                "SELECT count(*) FROM raw.kalshi_markets WHERE market_ticker LIKE 'INXD%'"
            ).fetchone()[0]
            cpi = con.execute(
                "SELECT count(*) FROM raw.kalshi_markets WHERE market_ticker LIKE 'CPI%'"
            ).fetchone()[0]
            self.assertEqual(inxd, 0)
            self.assertEqual(cpi, 0)

            inxd_trades = con.execute(
                "SELECT count(*) FROM raw.kalshi_market_snapshots WHERE market_ticker LIKE 'INXD%'"
            ).fetchone()[0]
            self.assertEqual(inxd_trades, 0)
        finally:
            con.close()

    def test_market_fields_mapped_correctly(self):
        extract_weather_history(
            kalshi_dir=self.tmp / 'kalshi',
            db_path=self.db_path,
        )
        con = duckdb.connect(str(self.db_path))
        try:
            row = con.execute("""
                SELECT market_ticker, event_ticker, title, subtitle, status, result
                FROM raw.kalshi_markets
                WHERE market_ticker = 'HIGHNY-25MAR19-B45.5'
            """).fetchone()
        finally:
            con.close()

        self.assertEqual(row[0], 'HIGHNY-25MAR19-B45.5')
        self.assertEqual(row[1], 'HIGHNY-25MAR19')
        self.assertIn('high temp', row[2].lower())
        self.assertEqual(row[3], '45° to 46°')
        self.assertEqual(row[4], 'finalized')
        self.assertEqual(row[5], 'yes')

    def test_trade_fields_mapped_correctly(self):
        extract_weather_history(
            kalshi_dir=self.tmp / 'kalshi',
            db_path=self.db_path,
        )
        con = duckdb.connect(str(self.db_path))
        try:
            row = con.execute("""
                SELECT market_ticker, last_price, volume
                FROM raw.kalshi_market_snapshots
                WHERE market_ticker = 'HIGHNY-25MAR19-B45.5'
                ORDER BY ts_utc
                LIMIT 1
            """).fetchone()
        finally:
            con.close()

        self.assertEqual(row[0], 'HIGHNY-25MAR19-B45.5')
        self.assertEqual(row[1], 45)  # yes_price
        self.assertEqual(row[2], 10)  # count


if __name__ == '__main__':
    unittest.main()
