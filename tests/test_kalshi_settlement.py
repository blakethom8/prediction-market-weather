import json
import tempfile
import unittest
from datetime import date
from pathlib import Path

from weatherlab.build.bootstrap import bootstrap
from weatherlab.db import connect
from weatherlab.live.live_orders import ORDERS_TO_SEED, fetch_live_orders, seed_live_order
from weatherlab.settlement.kalshi_settlement import (
    fetch_actual_high_from_kalshi,
    fix_march23_settlements,
    settle_live_order,
)


class StubKalshiClient:
    def __init__(self, *, market_results: dict[str, dict], series_markets: dict[str, list[dict]]):
        self.market_results = market_results
        self.series_markets = series_markets

    def _request_json(self, method: str, path: str, *, params=None, json_body=None):
        if method != 'GET':
            raise AssertionError(f'Unexpected method: {method}')
        if path.startswith('/markets/'):
            ticker = path.rsplit('/', 1)[-1]
            return {'market': self.market_results[ticker]}
        if path == '/markets':
            return {'markets': self.series_markets[params['series_ticker']]}
        raise AssertionError(f'Unexpected request: {method} {path}')


class KalshiSettlementTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / 'kalshi_settlement.duckdb'
        bootstrap(db_path=self.db_path)

    def tearDown(self):
        self.tmpdir.cleanup()

    def _insert_pending_calibration(
        self,
        *,
        live_order_id: str,
        market_date_local: date,
        city_key: str,
        station_id: str,
        ticker: str,
        our_forecast_f: float,
        forecast_confidence: str,
        market_ask_price: float,
        bucket_center_f: float,
        notes: dict,
    ) -> None:
        con = connect(db_path=self.db_path)
        try:
            con.execute(
                '''
                insert into ops.calibration_log (
                    log_id,
                    market_date_local,
                    city_key,
                    station_id,
                    ticker,
                    live_order_id,
                    is_paper_bet,
                    our_forecast_f,
                    forecast_confidence,
                    market_ask_price,
                    bucket_center_f,
                    notes
                ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                [
                    live_order_id,
                    market_date_local,
                    city_key,
                    station_id,
                    ticker,
                    live_order_id,
                    False,
                    our_forecast_f,
                    forecast_confidence,
                    market_ask_price,
                    bucket_center_f,
                    json.dumps(notes, sort_keys=True),
                ],
            )
        finally:
            con.close()

    def test_fetch_actual_high_from_kalshi_returns_between_bucket_midpoint(self):
        client = StubKalshiClient(
            market_results={},
            series_markets={
                'KXHIGHMIA': [
                    {
                        'ticker': 'KXHIGHMIA-26MAR23-B79.5',
                        'status': 'finalized',
                        'result': 'yes',
                    }
                ]
            },
        )

        actual_high_f = fetch_actual_high_from_kalshi('KXHIGHMIA', '26MAR23', client=client)

        self.assertEqual(actual_high_f, 79.5)

    def test_fetch_actual_high_from_kalshi_prefers_yes_bucket_when_threshold_loses(self):
        client = StubKalshiClient(
            market_results={},
            series_markets={
                'KXHIGHPHIL': [
                    {
                        'ticker': 'KXHIGHPHIL-26MAR23-T58',
                        'status': 'finalized',
                        'result': 'no',
                    },
                    {
                        'ticker': 'KXHIGHPHIL-26MAR23-B58.5',
                        'status': 'finalized',
                        'result': 'yes',
                    },
                ]
            },
        )

        actual_high_f = fetch_actual_high_from_kalshi('KXHIGHPHIL', '26MAR23', client=client)

        self.assertEqual(actual_high_f, 58.5)

    def test_settle_live_order_computes_yes_pnl_and_updates_db(self):
        live_order_id = seed_live_order(
            kalshi_order_id='order-lax-1',
            ticker='KXHIGHLAX-26MAR23-T76',
            action='buy',
            side='yes',
            order_type='limit',
            limit_price_cents=92,
            initial_count=5,
            fill_count=5,
            remaining_count=0,
            status='executed',
            taker_cost_dollars=4.60,
            db_path=self.db_path,
        )
        self._insert_pending_calibration(
            live_order_id=live_order_id,
            market_date_local=date(2026, 3, 23),
            city_key='lax',
            station_id='KLAX',
            ticker='KXHIGHLAX-26MAR23-T76',
            our_forecast_f=70.0,
            forecast_confidence='high',
            market_ask_price=0.92,
            bucket_center_f=76.0,
            notes={
                'market_favorite_ticker': 'KXHIGHLAX-26MAR23-T76',
                'market_favorite_center_f': 76.0,
            },
        )
        client = StubKalshiClient(
            market_results={
                'KXHIGHLAX-26MAR23-T76': {
                    'ticker': 'KXHIGHLAX-26MAR23-T76',
                    'status': 'finalized',
                    'result': 'yes',
                    'title': 'Will LAX stay below 76F?',
                }
            },
            series_markets={
                'KXHIGHLAX': [
                    {
                        'ticker': 'KXHIGHLAX-26MAR23-B70.5',
                        'status': 'finalized',
                        'result': 'yes',
                    }
                ]
            },
        )

        summary = settle_live_order(
            fetch_live_orders(db_path=self.db_path, live_order_id=live_order_id)[0],
            db_path=self.db_path,
            client=client,
        )

        self.assertTrue(summary['settled'])
        self.assertEqual(summary['outcome'], 'yes')
        self.assertAlmostEqual(summary['realized_pnl_dollars'], 0.40)
        self.assertEqual(summary['official_high_f'], 70.5)

        row = fetch_live_orders(db_path=self.db_path, live_order_id=live_order_id)[0]
        self.assertEqual(row['status'], 'settled')
        self.assertEqual(row['outcome_result'], 'yes')
        self.assertAlmostEqual(row['realized_pnl_dollars'], 0.40)

        con = connect(read_only=True, db_path=self.db_path)
        try:
            calibration = con.execute(
                '''
                select actual_high_f, outcome, edge_realized
                from ops.calibration_log
                where live_order_id = ?
                ''',
                [live_order_id],
            ).fetchone()
        finally:
            con.close()

        self.assertEqual(calibration[0], 70.5)
        self.assertEqual(calibration[1], 'yes')
        self.assertAlmostEqual(calibration[2], 0.40)

    def test_settle_live_order_computes_no_pnl_and_updates_db(self):
        live_order_id = seed_live_order(
            kalshi_order_id='order-phl-1',
            ticker='KXHIGHPHIL-26MAR23-T58',
            action='buy',
            side='yes',
            order_type='limit',
            limit_price_cents=1,
            initial_count=200,
            fill_count=200,
            remaining_count=0,
            status='executed',
            taker_cost_dollars=2.0,
            db_path=self.db_path,
        )
        self._insert_pending_calibration(
            live_order_id=live_order_id,
            market_date_local=date(2026, 3, 23),
            city_key='phl',
            station_id='KPHL',
            ticker='KXHIGHPHIL-26MAR23-T58',
            our_forecast_f=52.0,
            forecast_confidence='high',
            market_ask_price=0.01,
            bucket_center_f=58.0,
            notes={
                'market_favorite_ticker': 'KXHIGHPHIL-26MAR23-B58.5',
                'market_favorite_center_f': 58.5,
            },
        )
        client = StubKalshiClient(
            market_results={
                'KXHIGHPHIL-26MAR23-T58': {
                    'ticker': 'KXHIGHPHIL-26MAR23-T58',
                    'status': 'finalized',
                    'result': 'no',
                    'title': 'Will Philly stay below 58F?',
                }
            },
            series_markets={
                'KXHIGHPHIL': [
                    {
                        'ticker': 'KXHIGHPHIL-26MAR23-T58',
                        'status': 'finalized',
                        'result': 'no',
                    },
                    {
                        'ticker': 'KXHIGHPHIL-26MAR23-B58.5',
                        'status': 'finalized',
                        'result': 'yes',
                    },
                ]
            },
        )

        summary = settle_live_order(
            fetch_live_orders(db_path=self.db_path, live_order_id=live_order_id)[0],
            db_path=self.db_path,
            client=client,
        )

        self.assertTrue(summary['settled'])
        self.assertEqual(summary['outcome'], 'no')
        self.assertAlmostEqual(summary['realized_pnl_dollars'], -2.0)
        self.assertEqual(summary['official_high_f'], 58.5)

        row = fetch_live_orders(db_path=self.db_path, live_order_id=live_order_id)[0]
        self.assertEqual(row['status'], 'settled')
        self.assertEqual(row['outcome_result'], 'no')
        self.assertAlmostEqual(row['realized_pnl_dollars'], -2.0)

        con = connect(read_only=True, db_path=self.db_path)
        try:
            calibration = con.execute(
                '''
                select actual_high_f, outcome, edge_realized
                from ops.calibration_log
                where live_order_id = ?
                ''',
                [live_order_id],
            ).fetchone()
        finally:
            con.close()

        self.assertEqual(calibration[0], 58.5)
        self.assertEqual(calibration[1], 'no')
        self.assertAlmostEqual(calibration[2], -2.0)

    def test_fix_march23_settlements_corrects_all_march23_rows(self):
        seeded_ids: dict[str, str] = {}
        for order in ORDERS_TO_SEED:
            seeded_ids[order['kalshi_order_id']] = seed_live_order(db_path=self.db_path, **order)

        self._insert_pending_calibration(
            live_order_id=seeded_ids['3507f35c-c181-42cb-8053-6d303214c432'],
            market_date_local=date(2026, 3, 23),
            city_key='phl',
            station_id='KPHL',
            ticker='KXHIGHPHIL-26MAR23-T58',
            our_forecast_f=52.0,
            forecast_confidence='high',
            market_ask_price=0.01,
            bucket_center_f=58.0,
            notes={'market_favorite_ticker': 'KXHIGHPHIL-26MAR23-B58.5', 'market_favorite_center_f': 58.5},
        )
        self._insert_pending_calibration(
            live_order_id=seeded_ids['818c272a-fe15-484e-ac1b-325a3471be1d'],
            market_date_local=date(2026, 3, 23),
            city_key='dc',
            station_id='KDCA',
            ticker='KXHIGHTDC-26MAR23-T67',
            our_forecast_f=60.0,
            forecast_confidence='high',
            market_ask_price=0.01,
            bucket_center_f=67.0,
            notes={'market_favorite_ticker': 'KXHIGHTDC-26MAR23-B67.5', 'market_favorite_center_f': 67.5},
        )
        self._insert_pending_calibration(
            live_order_id=seeded_ids['da5a9204-59ba-4c4f-8ed5-eeb4d01d4918'],
            market_date_local=date(2026, 3, 23),
            city_key='bos',
            station_id='KBOS',
            ticker='KXHIGHTBOS-26MAR23-T36',
            our_forecast_f=34.0,
            forecast_confidence='high',
            market_ask_price=0.01,
            bucket_center_f=36.0,
            notes={'market_favorite_ticker': 'KXHIGHTBOS-26MAR23-B36.5', 'market_favorite_center_f': 36.5},
        )
        self._insert_pending_calibration(
            live_order_id=seeded_ids['f6b07664-2c39-44e4-bb90-6374f34d503b'],
            market_date_local=date(2026, 3, 23),
            city_key='hou',
            station_id='KHOU',
            ticker='KXHIGHTHOU-26MAR23-T83',
            our_forecast_f=80.0,
            forecast_confidence='high',
            market_ask_price=0.06,
            bucket_center_f=83.0,
            notes={'market_favorite_ticker': 'KXHIGHTHOU-26MAR23-B83.5', 'market_favorite_center_f': 83.5},
        )
        for kalshi_order_id in (
            'b4569b8e-4ec2-43ed-92b5-3e0185b35eae',
            'd229ceb2-08b0-4719-b48c-78ade09f0b7c',
            'f9836e23-a287-4655-9a4e-12d961d48936',
        ):
            ticker = next(order['ticker'] for order in ORDERS_TO_SEED if order['kalshi_order_id'] == kalshi_order_id)
            self._insert_pending_calibration(
                live_order_id=seeded_ids[kalshi_order_id],
                market_date_local=date(2026, 3, 23),
                city_key='miami',
                station_id='KMIA',
                ticker=ticker,
                our_forecast_f=79.0,
                forecast_confidence='high',
                market_ask_price=0.05,
                bucket_center_f=79.5 if ticker.endswith('B79.5') else 79.0,
                notes={'market_favorite_ticker': 'KXHIGHMIA-26MAR23-B82.5', 'market_favorite_center_f': 82.5},
            )

        con = connect(db_path=self.db_path)
        try:
            con.execute(
                '''
                update ops.live_orders
                set status = 'settled',
                    outcome_result = 'yes',
                    realized_pnl_dollars = 198.0
                where kalshi_order_id = '3507f35c-c181-42cb-8053-6d303214c432'
                '''
            )
            con.execute(
                '''
                update ops.calibration_log
                set actual_high_f = 51.98,
                    outcome = 'yes',
                    edge_realized = 198.0
                where live_order_id = ?
                ''',
                [seeded_ids['3507f35c-c181-42cb-8053-6d303214c432']],
            )
        finally:
            con.close()

        client = StubKalshiClient(
            market_results={
                'KXHIGHMIA-26MAR23-B79.5': {'ticker': 'KXHIGHMIA-26MAR23-B79.5', 'status': 'finalized', 'result': 'no'},
                'KXHIGHMIA-26MAR23-T79': {'ticker': 'KXHIGHMIA-26MAR23-T79', 'status': 'finalized', 'result': 'no'},
                'KXHIGHPHIL-26MAR23-T58': {'ticker': 'KXHIGHPHIL-26MAR23-T58', 'status': 'finalized', 'result': 'no'},
                'KXHIGHTBOS-26MAR23-T36': {'ticker': 'KXHIGHTBOS-26MAR23-T36', 'status': 'finalized', 'result': 'no'},
                'KXHIGHTDC-26MAR23-T67': {'ticker': 'KXHIGHTDC-26MAR23-T67', 'status': 'finalized', 'result': 'no'},
                'KXHIGHTHOU-26MAR23-T83': {'ticker': 'KXHIGHTHOU-26MAR23-T83', 'status': 'finalized', 'result': 'no'},
            },
            series_markets={
                'KXHIGHMIA': [
                    {'ticker': 'KXHIGHMIA-26MAR23-T79', 'status': 'finalized', 'result': 'no'},
                    {'ticker': 'KXHIGHMIA-26MAR23-B79.5', 'status': 'finalized', 'result': 'no'},
                    {'ticker': 'KXHIGHMIA-26MAR23-B82.5', 'status': 'finalized', 'result': 'yes'},
                ],
                'KXHIGHPHIL': [
                    {'ticker': 'KXHIGHPHIL-26MAR23-T58', 'status': 'finalized', 'result': 'no'},
                    {'ticker': 'KXHIGHPHIL-26MAR23-B58.5', 'status': 'finalized', 'result': 'yes'},
                ],
                'KXHIGHTBOS': [
                    {'ticker': 'KXHIGHTBOS-26MAR23-T36', 'status': 'finalized', 'result': 'no'},
                    {'ticker': 'KXHIGHTBOS-26MAR23-B36.5', 'status': 'finalized', 'result': 'yes'},
                ],
                'KXHIGHTDC': [
                    {'ticker': 'KXHIGHTDC-26MAR23-T67', 'status': 'finalized', 'result': 'no'},
                    {'ticker': 'KXHIGHTDC-26MAR23-B67.5', 'status': 'finalized', 'result': 'yes'},
                ],
                'KXHIGHTHOU': [
                    {'ticker': 'KXHIGHTHOU-26MAR23-T83', 'status': 'finalized', 'result': 'no'},
                    {'ticker': 'KXHIGHTHOU-26MAR23-B83.5', 'status': 'finalized', 'result': 'yes'},
                ],
            },
        )

        report = fix_march23_settlements(self.db_path, client=client)

        self.assertEqual(report['settled_count'], 8)
        self.assertAlmostEqual(report['total_realized_pnl'], -24.24)

        rows = fetch_live_orders(db_path=self.db_path)
        actual = {
            row['kalshi_order_id']: (row['status'], row['outcome_result'], row['realized_pnl_dollars'])
            for row in rows
        }
        self.assertEqual(actual['3507f35c-c181-42cb-8053-6d303214c432'], ('settled', 'no', -2.0))
        self.assertEqual(actual['818c272a-fe15-484e-ac1b-325a3471be1d'], ('settled', 'no', -2.0))
        self.assertEqual(actual['da5a9204-59ba-4c4f-8ed5-eeb4d01d4918'], ('settled', 'no', -2.0))
        self.assertEqual(actual['f6b07664-2c39-44e4-bb90-6374f34d503b'], ('settled', 'no', -5.28))
        self.assertEqual(actual['b4569b8e-4ec2-43ed-92b5-3e0185b35eae'], ('settled', 'no', -4.44))
        self.assertEqual(actual['d229ceb2-08b0-4719-b48c-78ade09f0b7c'], ('settled', 'no', -4.0))
        self.assertEqual(actual['f9836e23-a287-4655-9a4e-12d961d48936'], ('settled', 'no', -4.0))
        self.assertEqual(actual['a3675221-d090-4196-b812-b393ebfdb5f1'], ('settled', 'no', -0.52))

        con = connect(read_only=True, db_path=self.db_path)
        try:
            phl = con.execute(
                '''
                select actual_high_f, outcome, edge_realized
                from ops.calibration_log
                where live_order_id = ?
                ''',
                [seeded_ids['3507f35c-c181-42cb-8053-6d303214c432']],
            ).fetchone()
        finally:
            con.close()

        self.assertEqual(phl[0], 58.5)
        self.assertEqual(phl[1], 'no')
        self.assertAlmostEqual(phl[2], -2.0)


if __name__ == '__main__':
    unittest.main()
