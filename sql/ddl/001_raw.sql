-- Raw source tables

create schema if not exists raw;

create table if not exists raw.kalshi_markets (
    fetched_at_utc timestamp,
    source_file varchar,
    market_ticker varchar,
    event_ticker varchar,
    title varchar,
    subtitle varchar,
    rules_text varchar,
    status varchar,
    open_time_utc timestamp,
    close_time_utc timestamp,
    settlement_time_utc timestamp,
    result varchar,
    raw_json json
);

create table if not exists raw.kalshi_market_snapshots (
    market_ticker varchar,
    ts_utc timestamp,
    yes_bid double,
    yes_ask double,
    no_bid double,
    no_ask double,
    last_price double,
    volume double,
    open_interest double,
    raw_json json
);

create table if not exists raw.weather_forecasts (
    source varchar,
    city_id varchar,
    issued_at_utc timestamp,
    available_at_utc timestamp,
    target_date_local date,
    payload json
);

create table if not exists raw.weather_observations (
    source varchar,
    station_id varchar,
    ts_utc timestamp,
    payload json
);

create table if not exists raw.weather_settlement_reports (
    source varchar,
    station_id varchar,
    market_date_local date,
    published_at_utc timestamp,
    payload json
);
