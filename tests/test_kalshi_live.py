import base64
from datetime import UTC, datetime
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from weatherlab.build.bootstrap import bootstrap
from weatherlab.ingest.contracts import ingest_contract as seed_contract
from weatherlab.ingest.kalshi_live import (
    KalshiAuthError,
    KalshiClient,
    is_live_weather_ticker,
)
from weatherlab.ingest.kalshi_live_sync import sync_live_weather_markets

TEST_RSA_PRIVATE_KEY = """-----BEGIN PRIVATE KEY-----
MIICdwIBADANBgkqhkiG9w0BAQEFAASCAmEwggJdAgEAAoGBAMdiaz4SUTOdnoov
+dyulBeSb61zQkuf5Tc0ApElhm2t1f/EE4Sr/ZOB4Fp5V1C3gsqe53YhjktCcFlV
PVLfD08yc7v+tzezTcPJannnQ9s+mpUp4kPsgyXNc58yVkVIyvZuAasxTcA8oeXe
rrJPVwXlSdBRxrS+2wRq8a/J5diTAgMBAAECgYAw6Id99mhMzQEyzInyBDD3h0g1
t+wvOM21OODYUegjx1yTHLnc9YOCR32NM+6jMiC3KzuD0r9g3q9IeoxMY8MRLsFl
+aCqfzx5XFxGUULIOXtcb0lc1SeZHiPXNvcsDDn/8tEL7hui8gN1KihDVEN4+DLt
dcTgmlL0uzfk1I2vwQJBAPwck34vgydOmc/hfm4giiRMsnv2z6dxYtESshA1/4Vc
BmiDTIfLKlfHcMZ1x7N6IbphEoYMwsiCVmb1I2JuP0kCQQDKdahbvBAEl6VoNxal
xbi7+ZVO+2BreNoEQwfXb0Xmf38CW0rdnxruf0F5VpNmDr8qJXt5xjN0uqlUvrKN
Q2z7AkEA86YGYTg3z4AmJIKv9myaNRSuliFkdFWfg6FG12XoSZEzXEQwbThK9sR3
2EUxt+G7wO1ZwpWIldFpAV2+Ub1siQJATm395muYGO9WGGUe1OEfi1JIUOx4kamj
a3s8Emz8uyow3YzYF7qHCFUr3AF54FNeIsmaZ7YsQM/+wOGO8cJo+QJBAJ7evaT/
lVRH2bdhRU45rOfrVr2Ho7RcMeFad3m4k1yxD+dmsFLQ+PrBlmLZicmvz40K26Er
Bdjcw8tcKcHknS4=
-----END PRIVATE KEY-----
"""


class FakeResponse:
    def __init__(self, status_code: int, payload: object, headers: dict[str, str] | None = None):
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}
        self.text = '' if isinstance(payload, dict) else str(payload)

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


class FakeSession:
    def __init__(self, responses: list[FakeResponse]):
        self.responses = list(responses)
        self.requests: list[dict[str, object]] = []

    def request(self, method, url, params=None, headers=None, timeout=None):
        self.requests.append(
            {
                'method': method,
                'url': url,
                'params': params,
                'headers': headers,
                'timeout': timeout,
            }
        )
        return self.responses.pop(0)


class KalshiClientTests(unittest.TestCase):
    @unittest.skipUnless(shutil.which('openssl'), 'openssl is required for signing tests')
    def test_auth_header_generation_includes_timestamp_and_base64_signature(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            key_path = Path(tmpdir) / 'test.pem'
            key_path.write_text(TEST_RSA_PRIVATE_KEY)

            client = KalshiClient(
                key_id='dummy-key-id',
                private_key_path=key_path,
                clock_ms=lambda: 1700000000000,
            )

            headers = client._build_auth_headers('GET', '/markets')

        self.assertEqual(headers['KALSHI-ACCESS-KEY'], 'dummy-key-id')
        self.assertEqual(headers['KALSHI-ACCESS-TIMESTAMP'], '1700000000000')
        self.assertRegex(headers['KALSHI-ACCESS-SIGNATURE'], r'^[A-Za-z0-9+/]+=*$')
        decoded = base64.b64decode(headers['KALSHI-ACCESS-SIGNATURE'], validate=True)
        self.assertGreater(len(decoded), 0)

    def test_weather_market_filtering_merges_event_and_market_payloads(self):
        client = KalshiClient(key_id='dummy-key-id', private_key_path='unused.pem')

        market_page = [
            {
                'ticker': 'KXHIGHCHI-26MAR22-T70',
                'event_ticker': 'KXHIGHCHI-26MAR22',
                'title': 'Will the high temp in Chicago be above 70 on Mar 22, 2026?',
                'close_time': '2026-03-22T22:00:00Z',
                'yes_bid_dollars': '0.41',
                'yes_ask_dollars': '0.45',
                'last_price_dollars': '0.43',
                'volume_fp': '12.00',
                'open_interest_fp': '7.00',
            },
            {
                'ticker': 'INXD-26MAR22',
                'event_ticker': 'INXD-26MAR22',
                'title': 'Will the S&P 500 finish green?',
            },
        ]
        event_page = [
            {
                'ticker': 'KXHIGHCHI-26MAR22',
                'markets': [
                    {
                        'ticker': 'KXHIGHCHI-26MAR22-T70',
                        'title': 'Will the high temp in Chicago be above 70 on Mar 22, 2026?',
                        'rules_primary': 'Settlement uses the airport high.',
                        'settlement_ts': '2026-03-23T01:00:00Z',
                        'status': 'open',
                    },
                    {
                        'ticker': 'INXD-26MAR22',
                        'title': 'Ignore this non-weather market.',
                    },
                ],
            }
        ]

        with patch.object(client, '_get_paginated', side_effect=[market_page, event_page]):
            markets = client.fetch_open_weather_markets()

        self.assertEqual(len(markets), 1)
        market = markets[0]
        self.assertTrue(is_live_weather_ticker(market['ticker']))
        self.assertEqual(market['event_ticker'], 'KXHIGHCHI-26MAR22')
        self.assertEqual(market['status'], 'open')
        self.assertEqual(market['rules_text'], 'Settlement uses the airport high.')
        self.assertEqual(market['close_time'], datetime(2026, 3, 22, 22, 0, tzinfo=UTC))
        self.assertEqual(market['settlement_time'], datetime(2026, 3, 23, 1, 0, tzinfo=UTC))
        self.assertAlmostEqual(market['yes_bid'], 0.41)
        self.assertAlmostEqual(market['yes_ask'], 0.45)
        self.assertAlmostEqual(market['no_bid'], 0.55)
        self.assertAlmostEqual(market['no_ask'], 0.59)

    def test_live_weather_ticker_filter_accepts_current_t_series(self):
        self.assertTrue(is_live_weather_ticker('KXHIGHTDC-26MAR22-T70'))
        self.assertTrue(is_live_weather_ticker('KXHIGHTSEA-26MAR22-B60.5'))

    def test_empty_market_responses_return_empty_list(self):
        client = KalshiClient(key_id='dummy-key-id', private_key_path='unused.pem')

        with patch.object(client, '_get_paginated', side_effect=[[], []]):
            markets = client.fetch_open_weather_markets()

        self.assertEqual(markets, [])

    def test_auth_errors_raise_clean_exception(self):
        session = FakeSession(
            [FakeResponse(401, {'error': 'bad signature'})]
        )
        client = KalshiClient(
            key_id='dummy-key-id',
            private_key_path='unused.pem',
            session=session,
            signature_padding='pkcs1v15',
        )

        with patch.object(client, '_build_auth_headers', return_value={'Accept': 'application/json'}):
            with self.assertRaises(KalshiAuthError) as caught:
                client.fetch_market_snapshot('KXHIGHCHI-26MAR22-T70')

        self.assertIn('bad signature', str(caught.exception))


class KalshiLiveSyncTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / 'kalshi_live_test.duckdb'
        bootstrap(db_path=self.db_path)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_sync_calls_ingest_helpers_and_returns_summary(self):
        fake_client = Mock(spec=KalshiClient)
        fake_client.fetch_open_weather_markets.return_value = [
            {
                'ticker': 'KXHIGHCHI-26MAR22-T70',
                'event_ticker': 'KXHIGHCHI-26MAR22',
                'title': 'Will the high temp in Chicago be above 70 on Mar 22, 2026?',
                'rules_text': 'Settlement uses airport high',
                'status': 'open',
                'close_time': datetime(2026, 3, 22, 22, 0, tzinfo=UTC),
                'settlement_time': datetime(2026, 3, 23, 1, 0, tzinfo=UTC),
                'yes_bid': 0.41,
                'yes_ask': 0.45,
                'no_bid': 0.55,
                'no_ask': 0.59,
                'last_price': 0.43,
                'volume': 12.0,
                'open_interest': 7.0,
            }
        ]

        with patch('weatherlab.ingest.kalshi_live_sync.ingest_contract') as ingest_contract_mock:
            with patch('weatherlab.ingest.kalshi_live_sync.ingest_market_snapshot') as ingest_snapshot_mock:
                with patch('weatherlab.ingest.kalshi_live_sync.materialize_training_rows', return_value=0):
                    summary = sync_live_weather_markets(client=fake_client, db_path=self.db_path)

        ingest_contract_mock.assert_called_once()
        ingest_snapshot_mock.assert_called_once()
        self.assertEqual(
            set(summary),
            {
                'contracts_synced',
                'snapshots_synced',
                'board_size',
                'new_contracts',
                'updated_contracts',
            },
        )
        self.assertEqual(summary['contracts_synced'], 1)
        self.assertEqual(summary['snapshots_synced'], 1)
        self.assertEqual(summary['new_contracts'], 1)
        self.assertEqual(summary['updated_contracts'], 0)
        self.assertEqual(summary['board_size'], 0)
        self.assertEqual(
            ingest_contract_mock.call_args.kwargs['market_ticker'],
            'KXHIGHCHI-26MAR22-T70',
        )
        self.assertEqual(
            ingest_snapshot_mock.call_args.kwargs['market_ticker'],
            'KXHIGHCHI-26MAR22-T70',
        )

    def test_existing_contracts_are_counted_as_updates(self):
        seed_contract(
            market_ticker='KXHIGHCHI-26MAR22-T70',
            event_ticker='KXHIGHCHI-26MAR22',
            title='Will the high temp in Chicago be above 70 on Mar 22, 2026?',
            close_time_utc=datetime(2026, 3, 22, 22, 0, tzinfo=UTC),
            settlement_time_utc=datetime(2026, 3, 23, 1, 0, tzinfo=UTC),
            db_path=self.db_path,
        )
        fake_client = Mock(spec=KalshiClient)
        fake_client.fetch_open_weather_markets.return_value = [
            {
                'ticker': 'KXHIGHCHI-26MAR22-T70',
                'event_ticker': 'KXHIGHCHI-26MAR22',
                'title': 'Will the high temp in Chicago be above 70 on Mar 22, 2026?',
                'rules_text': '',
                'status': 'open',
                'close_time': datetime(2026, 3, 22, 22, 0, tzinfo=UTC),
                'settlement_time': datetime(2026, 3, 23, 1, 0, tzinfo=UTC),
                'yes_bid': 0.41,
                'yes_ask': 0.45,
                'no_bid': 0.55,
                'no_ask': 0.59,
                'last_price': 0.43,
                'volume': 12.0,
                'open_interest': 7.0,
            }
        ]

        with patch('weatherlab.ingest.kalshi_live_sync.ingest_contract'):
            with patch('weatherlab.ingest.kalshi_live_sync.ingest_market_snapshot'):
                with patch('weatherlab.ingest.kalshi_live_sync.materialize_training_rows', return_value=0):
                    summary = sync_live_weather_markets(client=fake_client, db_path=self.db_path)

        self.assertEqual(summary['new_contracts'], 0)
        self.assertEqual(summary['updated_contracts'], 1)


if __name__ == '__main__':
    unittest.main()
