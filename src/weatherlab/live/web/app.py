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
from ..queries import (
    get_dashboard_snapshot,
    get_latest_strategy_id,
    get_strategy_detail,
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


def _status_tone(value: str | None) -> str:
    mapping = {
        'approved': 'good',
        'priority': 'good',
        'closed': 'good',
        'YES': 'good',
        'pending_review': 'warn',
        'adjustments_requested': 'warn',
        'watch': 'warn',
        'open': 'warn',
        'rejected': 'bad',
        'pass': 'muted',
        'settled': 'muted',
        'converted_to_paper': 'muted',
    }
    return mapping.get(value or '', 'neutral')


def _short_text(value: str | None, limit: int = 100) -> str:
    if not value:
        return 'n/a'
    if len(value) <= limit:
        return value
    return value[: limit - 3].rstrip() + '...'


templates.env.filters['datetime_display'] = _format_datetime
templates.env.filters['probability_display'] = _format_probability
templates.env.filters['cents_display'] = _format_cents
templates.env.filters['edge_display'] = _format_edge
templates.env.filters['money_display'] = _format_money
templates.env.filters['status_tone'] = _status_tone
templates.env.filters['short_text'] = _short_text


def create_app(*, db_path: str | Path | None = None) -> FastAPI:
    resolved_db_path = Path(db_path).expanduser() if db_path else None
    bootstrap(db_path=resolved_db_path)

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

    @app.get('/', response_class=HTMLResponse, name='dashboard')
    def dashboard(request: Request) -> HTMLResponse:
        snapshot = get_dashboard_snapshot(db_path=request.app.state.db_path)
        return render(
            request,
            template_name='dashboard.html',
            page_title='Dashboard',
            nav='dashboard',
            context=snapshot,
        )

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
            },
        )

    @app.get('/healthz', response_class=JSONResponse, name='healthz')
    def healthz() -> JSONResponse:
        return JSONResponse({'status': 'ok'})

    return app
