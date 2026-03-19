-- Core normalized tables

create schema if not exists core;

create table if not exists core.cities (
    city_id varchar primary key,
    city_name varchar,
    timezone_name varchar,
    lat double,
    lon double,
    primary_station_id varchar
);

create table if not exists core.weather_stations (
    station_id varchar primary key,
    city_id varchar,
    station_name varchar,
    network varchar,
    timezone_name varchar,
    lat double,
    lon double,
    is_primary boolean,
    notes varchar
);

create table if not exists core.weather_contracts (
    contract_id varchar primary key,
    platform varchar,
    market_ticker varchar unique,
    event_ticker varchar,
    city_id varchar,
    station_id varchar,
    market_date_local date,
    timezone_name varchar,
    measure varchar,
    operator varchar,
    threshold_low_f double,
    threshold_high_f double,
    parse_status varchar,
    parse_confidence double,
    title varchar,
    rules_text varchar,
    close_time_utc timestamp,
    settlement_time_utc timestamp,
    status varchar,
    result varchar
);

create table if not exists core.market_snapshots (
    market_ticker varchar,
    ts_utc timestamp,
    price_yes_bid double,
    price_yes_ask double,
    price_yes_mid double,
    price_no_bid double,
    price_no_ask double,
    price_no_mid double,
    last_price double,
    spread_yes double,
    volume double,
    open_interest double,
    minutes_to_close integer,
    primary key (market_ticker, ts_utc)
);

create table if not exists core.forecast_snapshots (
    forecast_snapshot_id varchar primary key,
    source varchar,
    city_id varchar,
    issued_at_utc timestamp,
    available_at_utc timestamp,
    target_date_local date,
    pred_high_temp_f double,
    pred_low_temp_f double,
    pred_precip_prob double,
    summary_text varchar,
    raw_ref varchar
);

create table if not exists core.forecast_distributions (
    forecast_snapshot_id varchar,
    threshold_f double,
    prob_ge_threshold double,
    primary key (forecast_snapshot_id, threshold_f)
);

create table if not exists core.settlement_observations (
    settlement_id varchar primary key,
    source varchar,
    station_id varchar,
    city_id varchar,
    market_date_local date,
    observed_high_temp_f double,
    observed_low_temp_f double,
    observed_precip_in double,
    report_published_at_utc timestamp,
    is_final boolean,
    raw_ref varchar
);
