create table if not exists ops.live_orders (
    live_order_id varchar primary key,
    kalshi_order_id varchar unique,
    client_order_id varchar,
    strategy_id varchar,
    ticker varchar not null,
    action varchar not null,
    side varchar not null,
    order_type varchar not null,
    limit_price_cents integer,
    initial_count integer,
    fill_count integer default 0,
    remaining_count integer default 0,
    status varchar not null,
    taker_cost_dollars double,
    taker_fees_dollars double,
    outcome_result varchar,
    realized_pnl_dollars double,
    settlement_note varchar,
    created_at_utc timestamp,
    updated_at_utc timestamp,
    settled_at_utc timestamp
);

create or replace view ops.v_live_positions as
select
    ticker,
    side,
    sum(fill_count) as total_contracts,
    sum(fill_count * limit_price_cents) / 100.0 as total_cost_dollars,
    sum(fill_count) * 1.0 as max_payout_dollars,
    sum(fill_count * limit_price_cents) / 100.0 / nullif(sum(fill_count), 0) * 100 as avg_price_cents,
    count(*) as order_count,
    max(status) as latest_status,
    max(strategy_id) as strategy_id,
    sum(
        case
            when outcome_result = 'yes' then fill_count * (1.0 - limit_price_cents / 100.0)
            when outcome_result = 'no' then -fill_count * limit_price_cents / 100.0
            else null
        end
    ) as realized_pnl_dollars,
    max(outcome_result) as outcome_result
from ops.live_orders
where status != 'cancelled' or fill_count > 0
group by ticker, side;
