"""Local FastAPI app for the live betting platform."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from ...build.bootstrap import bootstrap
from ...db import connect
from .._shared import sum_numeric as _sum_numeric
from ..live_orders import fetch_live_orders, fetch_live_positions
from ..queries import (
    get_dashboard_snapshot,
    get_history_snapshot,
    get_latest_strategy_id,
    get_strategy_detail,
    get_today_snapshot,
    list_paper_bets,
    list_strategy_sessions,
    list_strategy_sessions_for_date,
)


PACKAGE_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = PACKAGE_DIR / 'templates'
STATIC_DIR = PACKAGE_DIR / 'static'

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


def _coerce_iso_date(value: str | date | None) -> date | None:
    if value is None or isinstance(value, date):
        return value
    return date.fromisoformat(value)


def _format_datetime(value: Any) -> str:
    if value in (None, ''):
        return 'n/a'
    if isinstance(value, datetime):
        return value.strftime('%Y-%m-%d %H:%M UTC')
    if isinstance(value, date):
        return value.isoformat()
    text = str(value)
    if 'T' in text:
        return text.replace('T', ' ')[:16] + ' UTC'
    return text


def _format_probability(value: float | None) -> str:
    if value is None:
        return 'n/a'
    return f'{value:.3f}'


def _format_cents(value: float | None) -> str:
    if value is None:
        return 'n/a'
    return f'{value * 100:.1f}c'


def _format_edge(value: float | None) -> str:
    if value is None:
        return 'n/a'
    return f'{value * 100:+.1f} pts'


def _format_money(value: float | None) -> str:
    if value is None:
        return 'n/a'
    return f'${value:,.2f}'


def _format_percent(value: float | None) -> str:
    if value is None:
        return 'n/a'
    return f'{value * 100:.0f}%'


def _status_label(value: str | None) -> str:
    if value in (None, ''):
        return 'n/a'
    return str(value).replace('_', ' ')


def _status_tone(value: str | None) -> str:
    mapping = {
        'approved': 'good',
        'priority': 'good',
        'closed': 'good',
        'executed': 'good',
        'YES': 'good',
        'buy': 'good',
        'pending_review': 'warn',
        'adjustments_requested': 'warn',
        'watch': 'warn',
        'open': 'warn',
        'pending': 'warn',
        'resting': 'warn',
        'sell': 'warn',
        'rejected': 'bad',
        'pass': 'muted',
        'settled': 'muted',
        'converted_to_paper': 'muted',
        'cancelled': 'muted',
        'unproposed': 'neutral',
        'yes': 'neutral',
        'no': 'neutral',
    }
    return mapping.get(value or '', 'neutral')


def _short_text(value: str | None, limit: int = 100) -> str:
    if not value:
        return 'n/a'
    if len(value) <= limit:
        return value
    return value[: limit - 3].rstrip() + '...'


def _notes_display(value: Any) -> str:
    if value in (None, '', {}, []):
        return 'n/a'
    if isinstance(value, dict):
        parts = []
        for key, item in value.items():
            if item in (None, '', {}, []):
                continue
            parts.append(f"{_status_label(str(key)).capitalize()}: {item}")
        return '; '.join(parts) if parts else 'n/a'
    if isinstance(value, (list, tuple, set)):
        parts = [str(item) for item in value if item not in (None, '')]
        return '; '.join(parts) if parts else 'n/a'
    return str(value)


def _build_paper_summary(
    *,
    strategy_filter: str | None,
    open_bets: list[dict[str, Any]],
    closed_bets: list[dict[str, Any]],
) -> dict[str, Any]:
    winning_bets = [row for row in closed_bets if (row.get('realized_pnl') or 0) > 0]
    losing_bets = [row for row in closed_bets if (row.get('realized_pnl') or 0) < 0]
    open_edges = [float(row['expected_edge']) for row in open_bets if row.get('expected_edge') is not None]
    return {
        'strategy_filter': strategy_filter,
        'open_notional': _sum_numeric(open_bets, 'notional_dollars'),
        'average_open_expected_edge': (sum(open_edges) / len(open_edges)) if open_edges else None,
        'closed_realized_pnl': _sum_numeric(closed_bets, 'realized_pnl'),
        'winning_bets': len(winning_bets),
        'losing_bets': len(losing_bets),
        'latest_lesson': next((row['lesson_summary'] for row in closed_bets if row.get('lesson_summary')), None),
    }


def _build_live_summary(
    *,
    strategy_filter: str | None,
    positions: list[dict[str, Any]],
    live_orders: list[dict[str, Any]],
) -> dict[str, Any]:
    open_positions = [row for row in positions if row.get('outcome_result') in (None, '')]
    settled_positions = [row for row in positions if row.get('outcome_result') not in (None, '')]
    return {
        'strategy_filter': strategy_filter,
        'total_contracts': _sum_numeric(positions, 'total_contracts'),
        'total_deployed_dollars': _sum_numeric(positions, 'total_cost_dollars'),
        'potential_max_payout_dollars': _sum_numeric(positions, 'max_payout_dollars'),
        'realized_pnl_dollars': _sum_numeric(settled_positions, 'realized_pnl_dollars'),
        'open_positions': len(open_positions),
        'settled_positions': len(settled_positions),
        'order_count': len(live_orders),
        'resting_orders': len([row for row in live_orders if row.get('status') == 'resting']),
        'latest_update': next((row.get('updated_at_utc') for row in live_orders if row.get('updated_at_utc')), None),
    }


templates.env.filters['datetime_display'] = _format_datetime
templates.env.filters['probability_display'] = _format_probability
templates.env.filters['cents_display'] = _format_cents
templates.env.filters['edge_display'] = _format_edge
templates.env.filters['money_display'] = _format_money
templates.env.filters['percent_display'] = _format_percent
templates.env.filters['status_label'] = _status_label
templates.env.filters['status_tone'] = _status_tone
templates.env.filters['short_text'] = _short_text
templates.env.filters['notes_display'] = _notes_display


def _assert_live_schema_ready(*, db_path: str | Path | None = None) -> None:
    con = connect(read_only=True, db_path=db_path)
    try:
        required_objects = {
            ('ops', 'strategy_sessions', 'BASE TABLE'),
            ('ops', 'strategy_market_board', 'BASE TABLE'),
            ('ops', 'bet_proposals', 'BASE TABLE'),
            ('ops', 'paper_bets', 'BASE TABLE'),
            ('ops', 'live_orders', 'BASE TABLE'),
            ('features', 'v_daily_market_board', 'VIEW'),
            ('ops', 'v_live_positions', 'VIEW'),
            ('ops', 'v_strategy_proposal_outcomes', 'VIEW'),
            ('ops', 'v_strategy_board_learning_history', 'VIEW'),
            ('ops', 'v_paper_bet_history', 'VIEW'),
            ('ops', 'v_strategy_session_learning', 'VIEW'),
        }
        rows = con.execute(
            '''
            select table_schema, table_name, table_type
            from information_schema.tables
            where (table_schema, table_name) in (
                ('ops', 'strategy_sessions'),
                ('ops', 'strategy_market_board'),
                ('ops', 'bet_proposals'),
                ('ops', 'paper_bets'),
                ('ops', 'live_orders'),
                ('features', 'v_daily_market_board'),
                ('ops', 'v_live_positions'),
                ('ops', 'v_strategy_proposal_outcomes'),
                ('ops', 'v_strategy_board_learning_history'),
                ('ops', 'v_paper_bet_history'),
                ('ops', 'v_strategy_session_learning')
            )
            '''
        ).fetchall()
    finally:
        con.close()

    found_objects = {(row[0], row[1], row[2]) for row in rows}
    missing = sorted(required_objects - found_objects)
    if missing:
        missing_labels = ', '.join(f'{schema}.{name} ({kind})' for schema, name, kind in missing)
        raise RuntimeError(f'Live schema is missing required objects: {missing_labels}')


def create_app(*, db_path: str | Path | None = None) -> FastAPI:
    resolved_db_path = Path(db_path).expanduser() if db_path else None
    bootstrap(db_path=resolved_db_path)
    _assert_live_schema_ready(db_path=resolved_db_path)

    app = FastAPI(title='Prediction Market Weather', docs_url=None, redoc_url=None)
    app.state.db_path = resolved_db_path
    app.mount('/static', StaticFiles(directory=str(STATIC_DIR)), name='static')

    def render(
        request: Request,
        *,
        template_name: str,
        page_title: str,
        nav: str,
        context: dict[str, Any],
        status_code: int = 200,
    ) -> HTMLResponse:
        latest_sessions = list_strategy_sessions(limit=1, db_path=request.app.state.db_path)
        merged_context = {
            'request': request,
            'page_title': page_title,
            'nav': nav,
            'latest_session': latest_sessions[0] if latest_sessions else None,
            'today': date.today().isoformat(),
            **context,
        }
        return templates.TemplateResponse(
            request=request,
            name=template_name,
            context=merged_context,
            status_code=status_code,
        )

    def render_board_page(
        request: Request,
        *,
        strategy_date: date | None,
        strategy_id: str | None,
    ) -> HTMLResponse:
        resolved_strategy_id = strategy_id or get_latest_strategy_id(
            strategy_date_local=strategy_date,
            db_path=request.app.state.db_path,
        )
        if resolved_strategy_id is None:
            requested_date = strategy_date.isoformat() if strategy_date else None
            return render(
                request,
                template_name='board.html',
                page_title='Daily Board',
                nav='board',
                context={
                    'detail': None,
                    'requested_date': requested_date,
                    'sessions_for_date': [],
                },
                status_code=404 if requested_date else 200,
            )

        detail = get_strategy_detail(strategy_id=resolved_strategy_id, db_path=request.app.state.db_path)
        if detail is None:
            raise HTTPException(status_code=404, detail=f'Unknown strategy session: {resolved_strategy_id}')

        selected_date = _coerce_iso_date(detail['session']['strategy_date_local'])
        sessions_for_date = list_strategy_sessions_for_date(
            strategy_date_local=selected_date,
            db_path=request.app.state.db_path,
        )
        return render(
            request,
            template_name='board.html',
            page_title='Daily Board',
            nav='board',
            context={
                'detail': detail,
                'requested_date': selected_date.isoformat() if selected_date else None,
                'sessions_for_date': sessions_for_date,
            },
        )

    def render_today_page(
        request: Request,
        *,
        strategy_id: str | None,
    ) -> HTMLResponse:
        snapshot = get_dashboard_snapshot(db_path=request.app.state.db_path)
        today_focus = get_today_snapshot(
            reference_date_local=date.today(),
            strategy_id=strategy_id,
            db_path=request.app.state.db_path,
        )
        return render(
            request,
            template_name='dashboard.html',
            page_title='Today',
            nav='today',
            context={
                **snapshot,
                'today_focus': today_focus,
            },
        )

    @app.get('/', response_class=HTMLResponse, name='dashboard')
    def dashboard(request: Request, strategy_id: str | None = Query(default=None)) -> HTMLResponse:
        return render_today_page(request, strategy_id=strategy_id)

    @app.get('/today', response_class=HTMLResponse, name='today_page')
    def today_page(request: Request, strategy_id: str | None = Query(default=None)) -> HTMLResponse:
        return render_today_page(request, strategy_id=strategy_id)

    @app.get('/board', response_class=HTMLResponse, name='latest_board')
    def latest_board(request: Request, strategy_id: str | None = Query(default=None)) -> HTMLResponse:
        return render_board_page(request, strategy_date=None, strategy_id=strategy_id)

    @app.get('/board/{strategy_date}', response_class=HTMLResponse, name='board_by_date')
    def board_by_date(
        request: Request,
        strategy_date: date,
        strategy_id: str | None = Query(default=None),
    ) -> HTMLResponse:
        return render_board_page(request, strategy_date=strategy_date, strategy_id=strategy_id)

    @app.get('/strategies/{strategy_id}', response_class=HTMLResponse, name='strategy_detail')
    def strategy_detail(request: Request, strategy_id: str) -> HTMLResponse:
        detail = get_strategy_detail(strategy_id=strategy_id, db_path=request.app.state.db_path)
        if detail is None:
            raise HTTPException(status_code=404, detail=f'Unknown strategy session: {strategy_id}')
        return render(
            request,
            template_name='strategy_detail.html',
            page_title='Strategy Session',
            nav='strategy',
            context=detail,
        )

    @app.get('/paper-bets', response_class=HTMLResponse, name='paper_bets_page')
    def paper_bets_page(
        request: Request,
        strategy_id: str | None = Query(default=None),
    ) -> HTMLResponse:
        paper_bets = list_paper_bets(strategy_id=strategy_id, db_path=request.app.state.db_path)
        open_bets = [row for row in paper_bets if row['status'] == 'open']
        closed_bets = [row for row in paper_bets if row['status'] == 'closed']
        return render(
            request,
            template_name='paper_bets.html',
            page_title='Paper Bets',
            nav='paper-bets',
            context={
                'strategy_filter': strategy_id,
                'paper_bets': paper_bets,
                'open_bets': open_bets,
                'closed_bets': closed_bets,
                'paper_summary': _build_paper_summary(
                    strategy_filter=strategy_id,
                    open_bets=open_bets,
                    closed_bets=closed_bets,
                ),
            },
        )

    @app.get('/live-orders', response_class=HTMLResponse, name='live_orders_page')
    def live_orders_page(
        request: Request,
        strategy_id: str | None = Query(default=None),
    ) -> HTMLResponse:
        positions = fetch_live_positions(db_path=request.app.state.db_path, strategy_id=strategy_id)
        live_orders = fetch_live_orders(db_path=request.app.state.db_path, strategy_id=strategy_id)
        return render(
            request,
            template_name='live_orders.html',
            page_title='Live Orders',
            nav='live-orders',
            context={
                'strategy_filter': strategy_id,
                'positions': positions,
                'live_orders': live_orders,
                'live_summary': _build_live_summary(
                    strategy_filter=strategy_id,
                    positions=positions,
                    live_orders=live_orders,
                ),
            },
        )

    @app.get('/history', response_class=HTMLResponse, name='history_page')
    def history_page(request: Request) -> HTMLResponse:
        history = get_history_snapshot(db_path=request.app.state.db_path)
        return render(
            request,
            template_name='history.html',
            page_title='History',
            nav='history',
            context={'history': history},
        )

    @app.get('/healthz', response_class=JSONResponse, name='healthz')
    def healthz(request: Request) -> JSONResponse:
        try:
            _assert_live_schema_ready(db_path=request.app.state.db_path)
        except RuntimeError as exc:
            return JSONResponse({'status': 'error', 'detail': str(exc)}, status_code=503)
        return JSONResponse({'status': 'ok'})

    return app
