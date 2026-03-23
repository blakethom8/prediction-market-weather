"""Internal helpers shared across live betting modules."""

from __future__ import annotations

import json
from typing import Any


def normalize_city_ids(city_ids: list[str] | tuple[str, ...] | None) -> list[str]:
    if not city_ids:
        return []
    return [city.strip().lower() for city in city_ids if city and city.strip()]


def json_dumps(payload: Any) -> str:
    return json.dumps({} if payload is None else payload)


def json_loads(payload: Any, *, default: Any) -> Any:
    if payload in (None, ''):
        return default
    if isinstance(payload, str):
        return json.loads(payload)
    return payload


def serialize_value(value: Any) -> Any:
    if hasattr(value, 'isoformat'):
        return value.isoformat()
    return value


def sum_numeric(rows: list[dict[str, Any]], key: str) -> float:
    return sum(float(row.get(key) or 0.0) for row in rows)
