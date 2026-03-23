from __future__ import annotations

import base64
from datetime import UTC, datetime
import json
import logging
from pathlib import Path
import shutil
import subprocess
import time
from typing import Any, Callable
from urllib.parse import urljoin, urlsplit

import requests

from ..settings import (
    KALSHI_API_BASE_URL,
    KALSHI_API_KEY_ID,
    KALSHI_API_PRIVATE_KEY_PATH,
    ROOT,
)

logger = logging.getLogger(__name__)

LIVE_WEATHER_TICKER_PREFIXES: tuple[str, ...] = (
    'KXCITIESWEATHER',
    'KXHIGHNYC',
    'KXHIGHCHI',
    'KXHIGHLAX',
    'KXHIGHHOU',
    'KXHIGHDEN',
    'KXHIGHPHIL',
    'KXHIGHMIA',
    'KXHIGHSEA',
    'KXHIGHDC',
    'KXHIGHBOS',
    'KXHIGHDET',
    'KXHIGHMIN',
    'KXHIGHATL',
    'KXHIGHDAL',
    'KXHIGHSF',
    'KXHIGHPHX',
)


class KalshiClientError(RuntimeError):
    """Base error for live Kalshi client failures."""


class KalshiConfigurationError(KalshiClientError):
    """Raised when Kalshi client settings are incomplete or invalid."""


class KalshiAuthError(KalshiClientError):
    """Raised when Kalshi authentication fails."""


class KalshiRateLimitError(KalshiClientError):
    """Raised when Kalshi rate limiting is encountered."""


class KalshiAPIError(KalshiClientError):
    """Raised for non-auth Kalshi API failures."""


def is_live_weather_ticker(ticker: str) -> bool:
    return any(ticker.startswith(prefix) for prefix in LIVE_WEATHER_TICKER_PREFIXES)


def _parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.strip()
    if normalized.endswith('Z'):
        normalized = normalized[:-1] + '+00:00'
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _parse_float(value: Any) -> float | None:
    if value in (None, ''):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coalesce(*values: Any) -> Any:
    for value in values:
        if value not in (None, ''):
            return value
    return None


def _complement_price(value: float | None) -> float | None:
    if value is None:
        return None
    return 1.0 - value if value <= 1.0 else 100.0 - value


def _extract_error_message(payload: Any) -> str:
    if isinstance(payload, dict):
        for key in ('message', 'error', 'detail'):
            value = payload.get(key)
            if value:
                return str(value)
    if isinstance(payload, list) and payload:
        return _extract_error_message(payload[0])
    if payload not in (None, ''):
        return str(payload)
    return 'No error payload returned.'


class KalshiClient:
    """Authenticated client for Kalshi's live trading API."""

    def __init__(
        self,
        *,
        key_id: str | None = None,
        private_key_path: str | Path | None = None,
        base_url: str | None = None,
        session: requests.Session | None = None,
        timeout_seconds: float = 15.0,
        clock_ms: Callable[[], int] | None = None,
        signature_padding: str = 'pss',
    ) -> None:
        self.key_id = key_id if key_id is not None else KALSHI_API_KEY_ID
        self.private_key_path = (
            Path(private_key_path)
            if private_key_path is not None
            else KALSHI_API_PRIVATE_KEY_PATH
        )
        self.base_url = (base_url or KALSHI_API_BASE_URL).rstrip('/')
        self.session = session or requests.Session()
        self.timeout_seconds = timeout_seconds
        self._clock_ms = clock_ms or (lambda: int(time.time() * 1000))
        self.signature_padding = signature_padding.lower()

        if self.signature_padding not in {'pss', 'pkcs1v15'}:
            raise KalshiConfigurationError(
                f'Unsupported Kalshi signature padding: {signature_padding}'
            )

    def fetch_open_weather_markets(self) -> list[dict[str, Any]]:
        markets_by_ticker: dict[str, dict[str, Any]] = {}
        errors: list[KalshiClientError] = []

        try:
            for market in self._get_paginated(
                '/markets',
                response_key='markets',
                params={'status': 'open', 'mve_filter': 'exclude'},
                page_size=1000,
            ):
                ticker = str(market.get('ticker') or '')
                if not is_live_weather_ticker(ticker):
                    continue
                markets_by_ticker[ticker] = self._normalize_market(market)
        except KalshiClientError as exc:
            errors.append(exc)

        try:
            for event in self._get_paginated(
                '/events',
                response_key='events',
                params={'status': 'open', 'with_nested_markets': 'true'},
                page_size=200,
            ):
                event_ticker = event.get('ticker')
                for market in event.get('markets') or []:
                    ticker = str(market.get('ticker') or '')
                    if not is_live_weather_ticker(ticker):
                        continue
                    normalized = self._normalize_market(
                        market,
                        event_ticker=event_ticker,
                    )
                    existing = markets_by_ticker.get(ticker, {})
                    merged = {
                        **existing,
                        **{
                            key: value
                            for key, value in normalized.items()
                            if value is not None
                        },
                    }
                    markets_by_ticker[ticker] = merged
        except KalshiClientError as exc:
            errors.append(exc)

        if not markets_by_ticker:
            if errors:
                if len(errors) == 1:
                    raise errors[0]
                raise KalshiAPIError('; '.join(str(error) for error in errors))
            logger.info('Kalshi returned no open weather markets.')
            return []

        if errors:
            logger.warning(
                'Kalshi live market fetch completed with partial data: %s',
                '; '.join(str(error) for error in errors),
            )

        return sorted(
            markets_by_ticker.values(),
            key=lambda row: (
                row.get('close_time') or datetime.max.replace(tzinfo=UTC),
                row.get('ticker') or '',
            ),
        )

    def fetch_market_snapshot(self, ticker: str) -> dict[str, Any] | None:
        payload = self._request_json(
            'GET',
            '/markets',
            params={'tickers': ticker, 'status': 'open', 'mve_filter': 'exclude'},
        )
        markets = payload.get('markets') or []
        for market in markets:
            if str(market.get('ticker') or '') == ticker:
                return self._normalize_market(market)
        return None

    def _normalize_market(
        self,
        payload: dict[str, Any],
        *,
        event_ticker: str | None = None,
    ) -> dict[str, Any]:
        yes_bid = _parse_float(_coalesce(payload.get('yes_bid_dollars'), payload.get('yes_bid')))
        yes_ask = _parse_float(_coalesce(payload.get('yes_ask_dollars'), payload.get('yes_ask')))
        no_bid = _parse_float(_coalesce(payload.get('no_bid_dollars'), payload.get('no_bid')))
        no_ask = _parse_float(_coalesce(payload.get('no_ask_dollars'), payload.get('no_ask')))

        if no_bid is None and yes_ask is not None:
            no_bid = _complement_price(yes_ask)
        if no_ask is None and yes_bid is not None:
            no_ask = _complement_price(yes_bid)

        return {
            'ticker': payload.get('ticker'),
            'event_ticker': _coalesce(payload.get('event_ticker'), event_ticker),
            'title': _coalesce(
                payload.get('title'),
                payload.get('subtitle'),
                payload.get('yes_sub_title'),
            ),
            'rules_text': '\n\n'.join(
                part
                for part in (
                    payload.get('rules_primary'),
                    payload.get('rules_secondary'),
                )
                if part
            ),
            'status': payload.get('status'),
            'close_time': _parse_timestamp(
                _coalesce(payload.get('close_time'), payload.get('expiration_time'))
            ),
            'settlement_time': _parse_timestamp(
                _coalesce(
                    payload.get('settlement_ts'),
                    payload.get('expected_expiration_time'),
                    payload.get('latest_expiration_time'),
                    payload.get('expiration_time'),
                )
            ),
            'yes_bid': yes_bid,
            'yes_ask': yes_ask,
            'no_bid': no_bid,
            'no_ask': no_ask,
            'last_price': _parse_float(
                _coalesce(payload.get('last_price_dollars'), payload.get('last_price'))
            ),
            'volume': _parse_float(_coalesce(payload.get('volume_fp'), payload.get('volume'))),
            'open_interest': _parse_float(
                _coalesce(payload.get('open_interest_fp'), payload.get('open_interest'))
            ),
        }

    def _get_paginated(
        self,
        path: str,
        *,
        response_key: str,
        params: dict[str, Any] | None = None,
        page_size: int,
    ) -> list[dict[str, Any]]:
        cursor: str | None = None
        results: list[dict[str, Any]] = []

        while True:
            page_params = dict(params or {})
            page_params.setdefault('limit', page_size)
            if cursor:
                page_params['cursor'] = cursor

            payload = self._request_json('GET', path, params=page_params)
            items = payload.get(response_key) or []
            if not isinstance(items, list):
                raise KalshiAPIError(
                    f'Unexpected Kalshi response shape for {path}: missing list field "{response_key}".'
                )
            results.extend(items)

            cursor = payload.get('cursor')
            if not cursor:
                break

        return results

    def _request_json(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        try:
            return self._request_json_once(method, path, params=params)
        except KalshiAuthError:
            if self.signature_padding != 'pss' or method.upper() != 'GET':
                raise

        logger.warning(
            'Kalshi auth failed with RSA-PSS for %s; retrying once with PKCS#1 v1.5.',
            path,
        )
        original_padding = self.signature_padding
        self.signature_padding = 'pkcs1v15'
        try:
            payload = self._request_json_once(method, path, params=params)
        except KalshiAuthError:
            self.signature_padding = original_padding
            raise

        logger.warning(
            'Kalshi auth succeeded with PKCS#1 v1.5 fallback; keeping compatibility mode.'
        )
        return payload

    def _request_json_once(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        full_url = self._build_url(path)
        headers = self._build_auth_headers(method, path)

        try:
            response = self.session.request(
                method=method,
                url=full_url,
                params=params,
                headers=headers,
                timeout=self.timeout_seconds,
            )
        except requests.RequestException as exc:
            raise KalshiAPIError(f'Kalshi request failed for {path}: {exc}') from exc

        payload: Any
        try:
            payload = response.json()
        except ValueError:
            payload = {'raw_text': response.text}

        if response.status_code in {401, 403}:
            raise KalshiAuthError(
                f'Kalshi authentication failed for {path}: {_extract_error_message(payload)}'
            )
        if response.status_code == 429:
            retry_after = response.headers.get('Retry-After')
            message = _extract_error_message(payload)
            if retry_after:
                message = f'{message} Retry-After={retry_after}'
            raise KalshiRateLimitError(f'Kalshi rate limit hit for {path}: {message}')
        if response.status_code >= 400:
            raise KalshiAPIError(
                f'Kalshi API error {response.status_code} for {path}: {_extract_error_message(payload)}'
            )
        if not isinstance(payload, dict):
            raise KalshiAPIError(
                f'Unexpected Kalshi response payload for {path}: {json.dumps(payload)[:200]}'
            )

        return payload

    def _build_auth_headers(self, method: str, path: str) -> dict[str, str]:
        if not self.key_id:
            raise KalshiConfigurationError('KALSHI_API_KEY_ID is not configured.')

        timestamp_ms = str(self._clock_ms())
        message = f'{timestamp_ms}{method.upper()}{self._signature_path(path)}'
        signature = self._sign_message(message)
        return {
            'Accept': 'application/json',
            'KALSHI-ACCESS-KEY': self.key_id,
            'KALSHI-ACCESS-TIMESTAMP': timestamp_ms,
            'KALSHI-ACCESS-SIGNATURE': signature,
        }

    def _build_url(self, path: str) -> str:
        base = f'{self.base_url}/'
        relative = path.lstrip('/')
        return urljoin(base, relative)

    def _signature_path(self, path: str) -> str:
        return urlsplit(self._build_url(path)).path

    def _resolve_private_key_path(self) -> Path:
        candidate = self.private_key_path.expanduser()
        if candidate.is_absolute():
            return candidate
        return ROOT / candidate

    def _sign_message(self, message: str) -> str:
        key_path = self._resolve_private_key_path()
        if not key_path.exists():
            raise KalshiConfigurationError(
                f'Kalshi private key file not found: {key_path}'
            )

        openssl_bin = shutil.which('openssl')
        if not openssl_bin:
            raise KalshiConfigurationError(
                'OpenSSL is required for Kalshi RSA signing but was not found on PATH.'
            )

        command = [
            openssl_bin,
            'dgst',
            '-sha256',
            '-sign',
            str(key_path),
        ]
        if self.signature_padding == 'pss':
            command.extend(
                [
                    '-sigopt',
                    'rsa_padding_mode:pss',
                    '-sigopt',
                    'rsa_pss_saltlen:digest',
                ]
            )

        result = subprocess.run(
            command,
            input=message.encode('utf-8'),
            capture_output=True,
            check=False,
        )
        if result.returncode != 0:
            stderr = result.stderr.decode('utf-8', errors='ignore').strip()
            raise KalshiConfigurationError(
                f'Kalshi signature generation failed for key {key_path}: {stderr or "openssl exited non-zero"}'
            )

        return base64.b64encode(result.stdout).decode('ascii')
