create or replace view features.v_latest_market_training_rows as
with ranked as (
    select
        ctr.*,
        row_number() over (
            partition by ctr.market_ticker
            order by ctr.ts_utc desc
        ) as rn
    from features.contract_training_rows ctr
)
select *
from ranked
where rn = 1;

create or replace view features.v_daily_market_board as
select
    market_ticker,
    ts_utc,
    city_id,
    market_date_local,
    latest_forecast_snapshot_id as forecast_snapshot_id,
    settlement_source,
    price_yes_mid,
    price_yes_ask,
    price_yes_bid,
    fair_prob,
    edge_vs_mid,
    edge_vs_ask,
    case
        when edge_vs_ask is null then 'insufficient_data'
        when edge_vs_ask >= 0.08 then 'priority'
        when edge_vs_ask >= 0.03 then 'watch'
        else 'pass'
    end as candidate_bucket,
    row_number() over (
        partition by market_date_local
        order by edge_vs_ask desc nulls last, abs(edge_vs_mid) desc nulls last
    ) as candidate_rank
from features.v_latest_market_training_rows;
