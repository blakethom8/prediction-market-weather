import tempfile
import unittest
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

from weatherlab.build.bootstrap import bootstrap
from weatherlab.build.registry_loader import load_all_registries
from weatherlab.build.training_rows import materialize_training_rows
from weatherlab.ingest.contracts import ingest_contract
from weatherlab.ingest.market_snapshots import ingest_market_snapshot
from weatherlab.ingest.open_meteo import ingest_open_meteo_daily_payload
from weatherlab.ingest.settlement_observations import ingest_settlement_observation
from weatherlab.live.persistence import create_paper_bet, settle_paper_bet
from weatherlab.live.workflow import apply_strategy_review, generate_daily_strategy_package

try:
    from fastapi.testclient import TestClient
    from weatherlab.live.web import create_app
except ImportError:  # pragma: no cover - exercised only when web deps are absent
    TestClient = None
    create_app = None


@unittest.skipUnless(TestClient is not None and create_app is not None, 'fastapi test dependencies are not installed')
class LiveWebAppTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / 'live_web.duckdb'
        self.artifacts_dir = Path(self.tmpdir.name) / 'artifacts'
        self.strategy_date = date.today()
        bootstrap(db_path=self.db_path)
        load_all_registries(db_path=self.db_path)
        self._seed_board()
        self.package = generate_daily_strategy_package(
            strategy_date_local=self.strategy_date,
            thesis='Compare the full daily board before approving paper bets.',
            research_focus_cities=['nyc', 'chi'],
            artifacts_dir=self.artifacts_dir,
            db_path=self.db_path,
        )
        apply_strategy_review(
            strategy_id=self.package['strategy_id'],
            decision='adjust',
            notes={'reason': 'Keep size small until near close.'},
            db_path=self.db_path,
        )
        self.primary_proposal = self.package['proposal_rows'][0]
        proposal_id = self.primary_proposal['proposal_id']
        paper_bet_id = create_paper_bet(
            strategy_id=self.package['strategy_id'],
            market_ticker=self.primary_proposal['market_ticker'],
            side='BUY_YES',
            limit_price=self.primary_proposal['target_price'],
            quantity=10,
            proposal_id=proposal_id,
            db_path=self.db_path,
        )
        settle_paper_bet(
            paper_bet_id=paper_bet_id,
            outcome_label='YES',
            review={'lesson_summary': 'Good edge capture after broad-board comparison.'},
            db_path=self.db_path,
        )
        self.client = TestClient(create_app(db_path=self.db_path))

    def tearDown(self):
        self.tmpdir.cleanup()

    def _seed_market(
        self,
        *,
        market_ticker: str,
        event_ticker: str,
        title: str,
        city_id: str,
        station_id: str,
        threshold_f: float,
        forecast_high_f: float,
        ask: float,
        bid: float,
        observed_high_f: float,
    ) -> None:
        ingest_contract(
            market_ticker=market_ticker,
            event_ticker=event_ticker,
            title=title,
            close_time_utc=datetime.combine(self.strategy_date, time(16, 0), tzinfo=UTC),
            settlement_time_utc=datetime.combine(self.strategy_date + timedelta(days=1), time(12, 0), tzinfo=UTC),
            db_path=self.db_path,
        )
        ingest_open_meteo_daily_payload(
            payload={
                'daily': {
                    'time': [self.strategy_date.isoformat()],
                    'temperature_2m_max': [forecast_high_f],
                    'temperature_2m_min': [43.0],
                    'precipitation_probability_max': [20],
                }
            },
            city_id=city_id,
            target_date_local=self.strategy_date,
            thresholds=[threshold_f],
            fetched_at_utc=datetime.combine(self.strategy_date, time(9, 0), tzinfo=UTC),
            db_path=self.db_path,
        )
        ingest_market_snapshot(
            market_ticker=market_ticker,
            ts_utc=datetime.combine(self.strategy_date, time(10, 0), tzinfo=UTC),
            price_yes_bid=bid,
            price_yes_ask=ask,
            price_no_bid=0.54,
            price_no_ask=0.58,
            last_price=(ask + bid) / 2,
            volume=100,
            open_interest=25,
            minutes_to_close=360,
            db_path=self.db_path,
        )
        ingest_settlement_observation(
            source='nws-cli',
            station_id=station_id,
            city_id=city_id,
            market_date_local=self.strategy_date,
            observed_high_temp_f=observed_high_f,
            report_published_at_utc=datetime.combine(self.strategy_date + timedelta(days=1), time(10, 0), tzinfo=UTC),
            db_path=self.db_path,
        )

    def _seed_board(self) -> None:
        title_date = f"{self.strategy_date.strftime('%b')} {self.strategy_date.day}, {self.strategy_date.year}"
        self._seed_market(
            market_ticker='TEST_NYC_54',
            event_ticker='TEST_EVENT_NYC',
            title=f'Will the high temp in NYC be above 54 on {title_date}?',
            city_id='nyc',
            station_id='KNYC',
            threshold_f=54.0,
            forecast_high_f=56.0,
            ask=0.45,
            bid=0.42,
            observed_high_f=57.0,
        )
        self._seed_market(
            market_ticker='TEST_CHI_58',
            event_ticker='TEST_EVENT_CHI',
            title=f'Will the high temp in Chicago be above 58 on {title_date}?',
            city_id='chi',
            station_id='KORD',
            threshold_f=58.0,
            forecast_high_f=63.0,
            ask=0.32,
            bid=0.30,
            observed_high_f=61.0,
        )
        materialize_training_rows(db_path=self.db_path)

    def test_dashboard_and_strategy_routes_render_seeded_data(self):
        dashboard = self.client.get('/')
        self.assertEqual(dashboard.status_code, 200)
        self.assertIn('board and recommendations', dashboard.text)
        self.assertIn('What should we do today?', dashboard.text)
        self.assertIn('Top recommendations right now', dashboard.text)
        self.assertIn(self.package['strategy_id'], dashboard.text)
        self.assertIn(self.primary_proposal['market_ticker'], dashboard.text)

        today_page = self.client.get('/today')
        self.assertEqual(today_page.status_code, 200)
        self.assertIn("Today's available candidate bets", today_page.text)
        self.assertIn('Proposal status and approval state', today_page.text)

        strategy = self.client.get(f"/strategies/{self.package['strategy_id']}")
        self.assertEqual(strategy.status_code, 200)
        self.assertIn('Ranked proposals', strategy.text)
        self.assertIn('Session decision state', strategy.text)
        self.assertIn('Adjustments were requested on this session.', strategy.text)
        self.assertIn('Keep size small until near close.', strategy.text)
        self.assertIn(self.primary_proposal['market_ticker'], strategy.text)

    def test_board_and_paper_bet_routes_render_seeded_data(self):
        board = self.client.get('/board')
        self.assertEqual(board.status_code, 200)
        self.assertIn('Captured market rows', board.text)
        self.assertIn('Top recommendations from this board', board.text)
        self.assertIn('TEST_CHI_58', board.text)
        self.assertIn('TEST_NYC_54', board.text)

        dated_board = self.client.get(f'/board/{self.strategy_date.isoformat()}')
        self.assertEqual(dated_board.status_code, 200)
        self.assertIn(self.package['strategy_id'], dated_board.text)

        paper_bets = self.client.get('/paper-bets')
        self.assertEqual(paper_bets.status_code, 200)
        self.assertIn('Settled outcomes and lessons', paper_bets.text)
        self.assertIn('Where to spend attention', paper_bets.text)
        self.assertIn('Good edge capture after broad-board comparison.', paper_bets.text)

        healthz = self.client.get('/healthz')
        self.assertEqual(healthz.status_code, 200)
        self.assertEqual(healthz.json(), {'status': 'ok'})


@unittest.skipUnless(TestClient is not None and create_app is not None, 'fastapi test dependencies are not installed')
class FreshBootstrapLiveWebAppTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / 'live_web_fresh.duckdb'
        self.artifacts_dir = Path(self.tmpdir.name) / 'artifacts'
        self.strategy_date = date.today()
        bootstrap(db_path=self.db_path)
        self.package = generate_daily_strategy_package(
            strategy_date_local=self.strategy_date,
            thesis='Create a review container even when the live board has no captured rows yet.',
            research_focus_cities=['nyc', 'chi'],
            artifacts_dir=self.artifacts_dir,
            db_path=self.db_path,
        )
        self.client = TestClient(create_app(db_path=self.db_path))

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_fresh_bootstrap_pages_render_without_seeded_market_data(self):
        dashboard = self.client.get('/')
        self.assertEqual(dashboard.status_code, 200)
        self.assertIn('board and recommendations', dashboard.text)
        self.assertIn('What should we do today?', dashboard.text)
        self.assertIn(self.package['strategy_id'], dashboard.text)

        today_page = self.client.get('/today')
        self.assertEqual(today_page.status_code, 200)
        self.assertIn("Today's available candidate bets", today_page.text)
        self.assertIn('No available candidate bets are left on the board right now.', today_page.text)

        board = self.client.get('/board')
        self.assertEqual(board.status_code, 200)
        self.assertIn('Captured market rows', board.text)
        self.assertIn('no market rows were captured', board.text.lower())

        strategy = self.client.get(f"/strategies/{self.package['strategy_id']}")
        self.assertEqual(strategy.status_code, 200)
        self.assertIn('Proposal to review chain', strategy.text)
        self.assertIn('Session decision state', strategy.text)
        self.assertIn('No proposals stored for this strategy session.', strategy.text)

        paper_bets = self.client.get('/paper-bets')
        self.assertEqual(paper_bets.status_code, 200)
        self.assertIn('No open paper bets right now.', paper_bets.text)
        self.assertIn('No paper bets have been recorded yet.', paper_bets.text)

        healthz = self.client.get('/healthz')
        self.assertEqual(healthz.status_code, 200)
        self.assertEqual(healthz.json(), {'status': 'ok'})


@unittest.skipUnless(TestClient is not None and create_app is not None, 'fastapi test dependencies are not installed')
class FallbackTodayLiveWebAppTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / 'live_web_fallback.duckdb'
        self.artifacts_dir = Path(self.tmpdir.name) / 'artifacts'
        self.reference_date = date.today()
        self.strategy_date = self.reference_date - timedelta(days=1)
        bootstrap(db_path=self.db_path)
        load_all_registries(db_path=self.db_path)
        self._seed_board()
        self.package = generate_daily_strategy_package(
            strategy_date_local=self.strategy_date,
            thesis='Use the latest broad-board package as fallback context until today is generated.',
            research_focus_cities=['nyc', 'chi'],
            artifacts_dir=self.artifacts_dir,
            db_path=self.db_path,
        )
        self.client = TestClient(create_app(db_path=self.db_path))

    def tearDown(self):
        self.tmpdir.cleanup()

    def _seed_market(
        self,
        *,
        market_ticker: str,
        event_ticker: str,
        title: str,
        city_id: str,
        station_id: str,
        threshold_f: float,
        forecast_high_f: float,
        ask: float,
        bid: float,
        observed_high_f: float,
    ) -> None:
        ingest_contract(
            market_ticker=market_ticker,
            event_ticker=event_ticker,
            title=title,
            close_time_utc=datetime.combine(self.strategy_date, time(16, 0), tzinfo=UTC),
            settlement_time_utc=datetime.combine(self.strategy_date + timedelta(days=1), time(12, 0), tzinfo=UTC),
            db_path=self.db_path,
        )
        ingest_open_meteo_daily_payload(
            payload={
                'daily': {
                    'time': [self.strategy_date.isoformat()],
                    'temperature_2m_max': [forecast_high_f],
                    'temperature_2m_min': [43.0],
                    'precipitation_probability_max': [20],
                }
            },
            city_id=city_id,
            target_date_local=self.strategy_date,
            thresholds=[threshold_f],
            fetched_at_utc=datetime.combine(self.strategy_date, time(9, 0), tzinfo=UTC),
            db_path=self.db_path,
        )
        ingest_market_snapshot(
            market_ticker=market_ticker,
            ts_utc=datetime.combine(self.strategy_date, time(10, 0), tzinfo=UTC),
            price_yes_bid=bid,
            price_yes_ask=ask,
            price_no_bid=0.54,
            price_no_ask=0.58,
            last_price=(ask + bid) / 2,
            volume=100,
            open_interest=25,
            minutes_to_close=360,
            db_path=self.db_path,
        )
        ingest_settlement_observation(
            source='nws-cli',
            station_id=station_id,
            city_id=city_id,
            market_date_local=self.strategy_date,
            observed_high_temp_f=observed_high_f,
            report_published_at_utc=datetime.combine(self.strategy_date + timedelta(days=1), time(10, 0), tzinfo=UTC),
            db_path=self.db_path,
        )

    def _seed_board(self) -> None:
        title_date = f"{self.strategy_date.strftime('%b')} {self.strategy_date.day}, {self.strategy_date.year}"
        self._seed_market(
            market_ticker='TEST_NYC_54',
            event_ticker='TEST_EVENT_NYC',
            title=f'Will the high temp in NYC be above 54 on {title_date}?',
            city_id='nyc',
            station_id='KNYC',
            threshold_f=54.0,
            forecast_high_f=56.0,
            ask=0.45,
            bid=0.42,
            observed_high_f=57.0,
        )
        self._seed_market(
            market_ticker='TEST_CHI_58',
            event_ticker='TEST_EVENT_CHI',
            title=f'Will the high temp in Chicago be above 58 on {title_date}?',
            city_id='chi',
            station_id='KORD',
            threshold_f=58.0,
            forecast_high_f=63.0,
            ask=0.32,
            bid=0.30,
            observed_high_f=61.0,
        )
        materialize_training_rows(db_path=self.db_path)

    def test_today_page_falls_back_to_latest_session_without_pretending_it_is_today(self):
        dashboard = self.client.get('/')
        self.assertEqual(dashboard.status_code, 200)
        self.assertIn(f'No strategy run stored for {self.reference_date.isoformat()} yet', dashboard.text)
        self.assertIn(f'Latest available context is {self.strategy_date.isoformat()}', dashboard.text)
        self.assertIn('Latest recommendations on file', dashboard.text)
        self.assertIn(f'Available candidate bets from {self.strategy_date.isoformat()}', dashboard.text)
        self.assertIn(f'Proposal status and approval state from {self.strategy_date.isoformat()}', dashboard.text)
        self.assertNotIn("Today's available candidate bets", dashboard.text)
        self.assertIn(self.package['strategy_id'], dashboard.text)

        today_page = self.client.get('/today')
        self.assertEqual(today_page.status_code, 200)
        self.assertIn(f'No strategy run stored for {self.reference_date.isoformat()} yet', today_page.text)
        self.assertIn(f'Available candidate bets from {self.strategy_date.isoformat()}', today_page.text)

        latest_board = self.client.get('/board')
        self.assertEqual(latest_board.status_code, 200)
        self.assertIn(self.package['strategy_id'], latest_board.text)

        missing_today_board = self.client.get(f'/board/{self.reference_date.isoformat()}')
        self.assertEqual(missing_today_board.status_code, 404)
        self.assertIn(f'There is no strategy session stored for {self.reference_date.isoformat()} yet.', missing_today_board.text)


@unittest.skipUnless(TestClient is not None and create_app is not None, 'fastapi test dependencies are not installed')
class EmptyLiveWebAppTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / 'live_web_empty.duckdb'
        self.today = date.today()
        bootstrap(db_path=self.db_path)
        self.client = TestClient(create_app(db_path=self.db_path))

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_pages_render_cleanly_when_no_strategy_data_exists(self):
        dashboard = self.client.get('/')
        self.assertEqual(dashboard.status_code, 200)
        self.assertIn('Today starts with the broad board', dashboard.text)
        self.assertIn('No strategy sessions yet.', dashboard.text)
        self.assertIn('No paper bets recorded yet.', dashboard.text)

        today_page = self.client.get('/today')
        self.assertEqual(today_page.status_code, 200)
        self.assertIn('Today starts with the broad board', today_page.text)

        board = self.client.get('/board')
        self.assertEqual(board.status_code, 200)
        self.assertIn('No board captured yet', board.text)

        dated_board = self.client.get(f'/board/{self.today.isoformat()}')
        self.assertEqual(dated_board.status_code, 404)
        self.assertIn(f'There is no strategy session stored for {self.today.isoformat()} yet.', dated_board.text)

        paper_bets = self.client.get('/paper-bets')
        self.assertEqual(paper_bets.status_code, 200)
        self.assertIn('No open paper bets right now.', paper_bets.text)
        self.assertIn('No paper bets have been recorded yet.', paper_bets.text)

        healthz = self.client.get('/healthz')
        self.assertEqual(healthz.status_code, 200)
        self.assertEqual(healthz.json(), {'status': 'ok'})


if __name__ == '__main__':
    unittest.main()
