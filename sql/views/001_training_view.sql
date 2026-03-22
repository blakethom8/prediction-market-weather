create or replace view features.v_training_rows as
with base as (
    select
        c.market_ticker,
        c.event_ticker,
        c.operator,
        c.threshold_low_f,
        c.threshold_high_f,
        m.ts_utc,
        c.city_id,
        c.market_date_local,
        -- Normalize prices to 0-1 scale (raw Kalshi data is in cents 0-100)
        coalesce(
            case when m.price_yes_mid > 1 then m.price_yes_mid / 100.0 else m.price_yes_mid end,
            case when m.last_price > 1 then m.last_price / 100.0 else m.last_price end
        ) as price_yes_mid,
        coalesce(
            case when m.price_yes_ask > 1 then m.price_yes_ask / 100.0 else m.price_yes_ask end,
            case when m.last_price > 1 then m.last_price / 100.0 else m.last_price end
        ) as price_yes_ask,
        coalesce(
            case when m.price_yes_bid > 1 then m.price_yes_bid / 100.0 else m.price_yes_bid end,
            case when m.last_price > 1 then m.last_price / 100.0 else m.last_price end
        ) as price_yes_bid,
        case when m.last_price > 1 then m.last_price / 100.0
             else m.last_price end as last_price,
        m.minutes_to_close,
        f.forecast_snapshot_id as latest_forecast_snapshot_id,
        -- P(temp >= threshold_low)
        fd_low.prob_ge_threshold as prob_ge_low,
        -- P(temp >= threshold_high + 1) for bracket probability
        fd_high.prob_ge_threshold as prob_ge_high,
        case
            when s.observed_high_temp_f is null then null
            when c.operator = 'between'
                and s.observed_high_temp_f >= c.threshold_low_f
                and s.observed_high_temp_f < c.threshold_high_f + 1 then 1
            when c.operator in ('>', '>=')
                and s.observed_high_temp_f >= c.threshold_low_f then 1
            when c.operator in ('<', '<=')
                and s.observed_high_temp_f <= c.threshold_low_f then 1
            else 0
        end as y_resolve_yes
    from core.weather_contracts c
    join core.market_snapshots m on m.market_ticker = c.market_ticker
    left join core.forecast_snapshots f
      on f.city_id = c.city_id
     and f.target_date_local = c.market_date_local
     and f.available_at_utc <= m.ts_utc
    left join core.forecast_distributions fd_low
      on fd_low.forecast_snapshot_id = f.forecast_snapshot_id
     and fd_low.threshold_f = c.threshold_low_f
    left join core.forecast_distributions fd_high
      on fd_high.forecast_snapshot_id = f.forecast_snapshot_id
     and fd_high.threshold_f = c.threshold_high_f + 1
    left join core.settlement_observations s
      on s.station_id = c.station_id
     and s.market_date_local = c.market_date_local
     and s.is_final = true
    qualify row_number() over (
        partition by c.market_ticker, m.ts_utc
        order by f.available_at_utc desc nulls last
    ) = 1
)
select
    market_ticker,
    event_ticker,
    ts_utc,
    city_id,
    market_date_local,
    price_yes_mid,
    price_yes_ask,
    price_yes_bid,
    last_price,
    minutes_to_close,
    latest_forecast_snapshot_id,
    -- Fair probability: bracket prob for 'between', P(>=threshold) for >=, 1-P(>=threshold) for <=
    case
        when operator = 'between' and prob_ge_low is not null and prob_ge_high is not null
            then prob_ge_low - prob_ge_high
        when operator = 'between' and prob_ge_low is not null
            then prob_ge_low
        when operator in ('>', '>=') then prob_ge_low
        when operator in ('<', '<=') then 1.0 - prob_ge_low
        else prob_ge_low
    end as fair_prob,
    case
        when operator = 'between' and prob_ge_low is not null and prob_ge_high is not null
            then (prob_ge_low - prob_ge_high) - price_yes_mid
        when operator in ('>', '>=') and prob_ge_low is not null
            then prob_ge_low - price_yes_mid
        when operator in ('<', '<=') and prob_ge_low is not null
            then (1.0 - prob_ge_low) - price_yes_mid
        else null
    end as edge_vs_mid,
    case
        when operator = 'between' and prob_ge_low is not null and prob_ge_high is not null
            then (prob_ge_low - prob_ge_high) - price_yes_ask
        when operator in ('>', '>=') and prob_ge_low is not null
            then prob_ge_low - price_yes_ask
        when operator in ('<', '<=') and prob_ge_low is not null
            then (1.0 - prob_ge_low) - price_yes_ask
        else null
    end as edge_vs_ask,
    y_resolve_yes
from base;
