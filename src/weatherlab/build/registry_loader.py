from __future__ import annotations

from pathlib import Path

import yaml

from ..db import connect
from ..settings import CONFIG_DIR


def _load_yaml(path: Path) -> dict:
    with path.open('r', encoding='utf-8') as f:
        return yaml.safe_load(f) or {}


def load_city_registry(db_path: str | Path | None = None) -> int:
    payload = _load_yaml(CONFIG_DIR / 'cities.yml')
    rows = payload.get('cities', [])
    con = connect(db_path=db_path)
    try:
        for row in rows:
            con.execute(
                '''
                insert into core.cities (
                    city_id, city_name, timezone_name, lat, lon, primary_station_id
                ) values (?, ?, ?, ?, ?, ?)
                on conflict(city_id) do update set
                    city_name = excluded.city_name,
                    timezone_name = excluded.timezone_name,
                    lat = excluded.lat,
                    lon = excluded.lon,
                    primary_station_id = excluded.primary_station_id
                ''',
                [
                    row['city_id'],
                    row['city_name'],
                    row['timezone_name'],
                    row.get('lat'),
                    row.get('lon'),
                    row.get('primary_station_id'),
                ],
            )
    finally:
        con.close()
    return len(rows)


def load_station_registry(db_path: str | Path | None = None) -> int:
    payload = _load_yaml(CONFIG_DIR / 'stations.yml')
    rows = payload.get('stations', [])
    con = connect(db_path=db_path)
    try:
        for row in rows:
            con.execute(
                '''
                insert into core.weather_stations (
                    station_id, city_id, station_name, network, timezone_name, lat, lon, is_primary, notes
                ) values (?, ?, ?, ?, ?, ?, ?, ?, ?)
                on conflict(station_id) do update set
                    city_id = excluded.city_id,
                    station_name = excluded.station_name,
                    network = excluded.network,
                    timezone_name = excluded.timezone_name,
                    lat = excluded.lat,
                    lon = excluded.lon,
                    is_primary = excluded.is_primary,
                    notes = excluded.notes
                ''',
                [
                    row['station_id'],
                    row['city_id'],
                    row['station_name'],
                    row.get('network'),
                    row.get('timezone_name'),
                    row.get('lat'),
                    row.get('lon'),
                    row.get('is_primary', False),
                    row.get('notes'),
                ],
            )
    finally:
        con.close()
    return len(rows)


def load_all_registries(db_path: str | Path | None = None) -> dict[str, int]:
    return {
        'cities': load_city_registry(db_path=db_path),
        'stations': load_station_registry(db_path=db_path),
    }


if __name__ == '__main__':
    print(load_all_registries())
