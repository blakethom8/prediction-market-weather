from __future__ import annotations

import argparse
import os

import uvicorn

from .app import create_app


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Run the live betting web app.')
    parser.add_argument('--host', default=os.environ.get('WEATHER_LIVE_WEB_HOST', '0.0.0.0'))
    parser.add_argument('--port', type=int, default=int(os.environ.get('WEATHER_LIVE_WEB_PORT', '8000')))
    parser.add_argument('--reload', action='store_true', help='Enable auto-reload for local editing')
    parser.add_argument('--db-path', default=os.environ.get('WEATHER_WAREHOUSE_PATH'))
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    app = create_app(db_path=args.db_path)
    uvicorn.run(app, host=args.host, port=args.port, reload=args.reload)


if __name__ == '__main__':
    main()
