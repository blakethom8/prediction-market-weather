create table if not exists ops.calibration_log (
    log_id varchar primary key,
    market_date_local date not null,
    city_key varchar not null,
    station_id varchar not null,
    ticker varchar not null,
    live_order_id varchar,
    is_paper_bet boolean default false,
    our_forecast_f double,
    forecast_confidence varchar,
    market_ask_price double,
    bucket_center_f double,
    actual_high_f double,
    outcome varchar,
    forecast_error_f double,
    market_was_right boolean,
    edge_realized double,
    notes varchar,
    created_at_utc timestamp default current_timestamp
);

create or replace view ops.v_calibration_summary as
select
    forecast_confidence,
    city_key,
    count(*) as total_bets,
    avg(abs(forecast_error_f)) as avg_abs_error_f,
    sum(case when outcome = 'yes' then 1 else 0 end)::float / count(*) as win_rate,
    avg(edge_realized) as avg_realized_edge,
    sum(edge_realized) as total_pnl
from ops.calibration_log
where actual_high_f is not null
group by forecast_confidence, city_key;
