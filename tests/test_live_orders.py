import tempfile
import unittest
from datetime import UTC, date, datetime
from pathlib import Path
from unittest.mock import Mock, patch

from weatherlab.build.bootstrap import bootstrap
from weatherlab.db import connect
from weatherlab.live.live_orders import (
    ORDERS_TO_SEED,
    fetch_live_orders,
    fetch_live_positions,
    seed_live_order,
    seed_tonights_live_orders,
    settle_live_order,
    sync_all_open_live_orders,
    sync_live_order_from_kalshi,
)

try:
    from fastapi.testclient import TestClient
    from weatherlab.live.web import create_app
except ImportError:  # pragma: no cover - exercised only when web deps are absent
    TestClient = None
    create_app = None


class LiveOrderPersistenceTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / 'live_orders.duckdb'
        bootstrap(db_path=self.db_path)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_seed_live_order_inserts_once_and_is_idempotent(self):
        order = ORDERS_TO_SEED[1]

        first_id = seed_live_order(db_path=self.db_path, **order)
        second_id = seed_live_order(db_path=self.db_path, **order)

        self.assertEqual(first_id, second_id)
        rows = fetch_live_orders(db_path=self.db_path)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['kalshi_order_id'], order['kalshi_order_id'])
        self.assertEqual(rows[0]['created_at_utc'], '2026-03-23T07:42:36')

    def test_settle_live_order_sets_outcome_and_realized_pnl(self):
        seed_live_order(
            kalshi_order_id='order-settle-1',
            client_order_id='manual-1774251756',
            strategy_id='strategy_test_settle',
            ticker='KXHIGHSEA-26MAR23-T60',
            action='buy',
            side='yes',
            order_type='limit',
            limit_price_cents=25,
            initial_count=10,
            fill_count=10,
            remaining_count=0,
            status='executed',
            db_path=self.db_path,
        )

        settle_live_order('order-settle-1', 'yes', settlement_note='Airport report confirmed.', db_path=self.db_path)

        row = fetch_live_orders(db_path=self.db_path, kalshi_order_id='order-settle-1')[0]
        self.assertEqual(row['status'], 'settled')
        self.assertEqual(row['outcome_result'], 'yes')
        self.assertEqual(row['settlement_note'], 'Airport report confirmed.')
        self.assertAlmostEqual(row['realized_pnl_dollars'], 7.50)
        self.assertIsNotNone(row['settled_at_utc'])

    def test_fetch_live_positions_aggregates_multiple_orders_on_same_ticker(self):
        for order in ORDERS_TO_SEED[:3]:
            seed_live_order(db_path=self.db_path, **order)

        positions = fetch_live_positions(db_path=self.db_path)
        self.assertEqual(len(positions), 1)
        position = positions[0]
        self.assertEqual(position['ticker'], 'KXHIGHMIA-26MAR23-B79.5')
        self.assertEqual(position['side'], 'yes')
        self.assertEqual(position['total_contracts'], 167)
        self.assertEqual(position['order_count'], 3)
        self.assertAlmostEqual(position['total_cost_dollars'], 8.96)
        self.assertAlmostEqual(position['max_payout_dollars'], 167.0)
        self.assertAlmostEqual(position['avg_price_cents'], 5.365269461077844)
        self.assertIsNone(position['outcome_result'])

    def test_empty_state_fetchers_return_no_rows(self):
        self.assertEqual(fetch_live_orders(db_path=self.db_path), [])
        self.assertEqual(fetch_live_positions(db_path=self.db_path), [])

    def test_sync_live_order_from_kalshi_updates_fill_status(self):
        seed_live_order(db_path=self.db_path, **ORDERS_TO_SEED[-1])
        mock_client = Mock()
        mock_client._request_json.return_value = {
            'order': {
                'order_id': ORDERS_TO_SEED[-1]['kalshi_order_id'],
                'status': 'executed',
                'fill_count': 100,
                'remaining_count': 0,
                'limit_price': 6,
                'taker_cost_dollars': 6.0,
                'taker_fees_dollars': 0.12,
                'updated_at': '2026-03-23T08:15:00Z',
            }
        }

        with patch('weatherlab.live.live_orders.KalshiClient', return_value=mock_client):
            updated = sync_live_order_from_kalshi(ORDERS_TO_SEED[-1]['kalshi_order_id'], db_path=self.db_path)

        self.assertEqual(updated['status'], 'executed')
        self.assertEqual(updated['fill_count'], 100)
        self.assertEqual(updated['remaining_count'], 0)
        self.assertAlmostEqual(updated['taker_fees_dollars'], 0.12)
        mock_client._request_json.assert_called_once_with(
            'GET',
            f"/portfolio/orders/{ORDERS_TO_SEED[-1]['kalshi_order_id']}",
        )

    def test_sync_all_open_live_orders_only_syncs_pending_or_resting_rows(self):
        seed_live_order(db_path=self.db_path, **ORDERS_TO_SEED[-1])
        seed_live_order(db_path=self.db_path, **ORDERS_TO_SEED[1])
        mock_client = Mock()
        mock_client._request_json.return_value = {
            'order': {
                'order_id': ORDERS_TO_SEED[-1]['kalshi_order_id'],
                'status': 'executed',
                'fill_count': 100,
                'remaining_count': 0,
                'updated_at': '2026-03-23T08:20:00Z',
            }
        }

        with patch('weatherlab.live.live_orders.KalshiClient', return_value=mock_client):
            updates = sync_all_open_live_orders(db_path=self.db_path)

        self.assertEqual(len(updates), 1)
        self.assertEqual(updates[0]['kalshi_order_id'], ORDERS_TO_SEED[-1]['kalshi_order_id'])
        mock_client._request_json.assert_called_once()


@unittest.skipUnless(TestClient is not None and create_app is not None, 'fastapi test dependencies are not installed')
class LiveOrderWebTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / 'live_orders_web.duckdb'
        bootstrap(db_path=self.db_path)
        self._insert_strategy_session('strategy_ad19954fb914', 'Miami ladder and paired trigger order.')
        self._insert_strategy_session('strategy_159766531730', 'Cheap weather tails across multiple airports.')
        seed_tonights_live_orders(db_path=self.db_path)
        self.client = TestClient(create_app(db_path=self.db_path))

    def tearDown(self):
        self.tmpdir.cleanup()

    def _insert_strategy_session(self, strategy_id: str, thesis: str) -> None:
        con = connect(db_path=self.db_path)
        try:
            con.execute(
                '''
                insert into ops.strategy_sessions (
                    strategy_id,
                    created_at_utc,
                    strategy_date_local,
                    status,
                    approval_status,
                    focus_cities_json,
                    research_focus_cities_json,
                    board_scope,
                    board_market_count,
                    board_city_count,
                    thesis,
                    selection_framework_json,
                    strategy_variant,
                    scenario_label,
                    session_context_json,
                    notes_json
                ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                [
                    strategy_id,
                    datetime(2026, 3, 23, 8, 0, tzinfo=UTC),
                    date(2026, 3, 23),
                    'draft',
                    'approved',
                    '[]',
                    '[]',
                    'all_markets',
                    0,
                    0,
                    thesis,
                    '{}',
                    'baseline',
                    'live',
                    '{}',
                    '{}',
                ],
            )
        finally:
            con.close()

    def test_live_orders_route_and_strategy_detail_render_seeded_data(self):
        response = self.client.get('/live-orders')
        self.assertEqual(response.status_code, 200)
        self.assertIn('Live orders and positions', response.text)
        self.assertIn('KXHIGHMIA-26MAR23-B79.5', response.text)
        self.assertIn('March 24, 2026 settlement workflow', response.text)
        self.assertIn('Tuesday morning, March 24, 2026', response.text)

        strategy = self.client.get('/strategies/strategy_ad19954fb914')
        self.assertEqual(strategy.status_code, 200)
        self.assertIn('Live orders linked to this strategy', strategy.text)
        self.assertIn('b4569b8e-4ec2-43ed-92b5-3e0185b35eae', strategy.text)

    def test_live_orders_pages_render_empty_state_when_no_orders_exist(self):
        empty_db = Path(self.tmpdir.name) / 'live_orders_empty.duckdb'
        bootstrap(db_path=empty_db)
        con = connect(db_path=empty_db)
        try:
            con.execute(
                '''
                insert into ops.strategy_sessions (
                    strategy_id,
                    created_at_utc,
                    strategy_date_local,
                    status,
                    approval_status,
                    thesis
                ) values (?, ?, ?, ?, ?, ?)
                ''',
                [
                    'strategy_empty_live_orders',
                    datetime(2026, 3, 23, 9, 0, tzinfo=UTC),
                    date(2026, 3, 23),
                    'draft',
                    'pending_review',
                    'Check empty-state rendering for real-money pages.',
                ],
            )
        finally:
            con.close()

        client = TestClient(create_app(db_path=empty_db))

        response = client.get('/live-orders')
        self.assertEqual(response.status_code, 200)
        self.assertIn('No live orders recorded yet.', response.text)

        strategy = client.get('/strategies/strategy_empty_live_orders')
        self.assertEqual(strategy.status_code, 200)
        self.assertIn('No live orders placed under this strategy session.', strategy.text)


if __name__ == '__main__':
    unittest.main()
