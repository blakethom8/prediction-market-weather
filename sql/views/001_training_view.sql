create or replace view features.v_training_rows as
select
    c.market_ticker,
    m.ts_utc,
    c.city_id,
    c.market_date_local,
    m.price_yes_mid,
    m.price_yes_ask,
    m.price_yes_bid,
    m.minutes_to_close,
    f.forecast_snapshot_id as latest_forecast_snapshot_id,
    fd.prob_ge_threshold as fair_prob,
    fd.prob_ge_threshold - m.price_yes_mid as edge_vs_mid,
    fd.prob_ge_threshold - m.price_yes_ask as edge_vs_ask,
    case
        when s.observed_high_temp_f is null then null
        when c.operator in ('>', '>=') and s.observed_high_temp_f >= c.threshold_low_f then 1
        when c.operator in ('<', '<=') and s.observed_high_temp_f <= c.threshold_low_f then 1
        else 0
    end as y_resolve_yes
from core.weather_contracts c
join core.market_snapshots m on m.market_ticker = c.market_ticker
left join core.forecast_snapshots f
  on f.city_id = c.city_id
 and f.target_date_local = c.market_date_local
 and f.available_at_utc <= m.ts_utc
left join core.forecast_distributions fd
  on fd.forecast_snapshot_id = f.forecast_snapshot_id
 and fd.threshold_f = c.threshold_low_f
left join core.settlement_observations s
  on s.station_id = c.station_id
 and s.market_date_local = c.market_date_local
 and s.is_final = true
qualify row_number() over (
    partition by c.market_ticker, m.ts_utc
    order by f.available_at_utc desc nulls last
) = 1;
