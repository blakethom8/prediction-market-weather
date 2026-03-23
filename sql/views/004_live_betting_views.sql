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
    tr.market_ticker,
    c.title as market_title,
    tr.ts_utc,
    tr.city_id,
    tr.market_date_local,
    tr.minutes_to_close,
    tr.latest_forecast_snapshot_id as forecast_snapshot_id,
    tr.price_yes_mid,
    tr.price_yes_ask,
    tr.price_yes_bid,
    tr.fair_prob,
    tr.edge_vs_mid,
    tr.edge_vs_ask,
    case
        when tr.edge_vs_ask is null then 'insufficient_data'
        when tr.edge_vs_ask >= 0.08 then 'priority'
        when tr.edge_vs_ask >= 0.03 then 'watch'
        else 'pass'
    end as candidate_bucket,
    row_number() over (
        partition by tr.market_date_local
        order by tr.edge_vs_ask desc nulls last, abs(tr.edge_vs_mid) desc nulls last
    ) as candidate_rank
from features.v_latest_market_training_rows tr
left join core.weather_contracts c on c.market_ticker = tr.market_ticker;

create or replace view ops.v_strategy_proposal_outcomes as
with latest_reviews as (
    select *
    from (
        select
            r.*,
            row_number() over (
                partition by r.paper_bet_id
                order by r.reviewed_at_utc desc nulls last, r.review_id desc
            ) as rn
        from ops.paper_bet_reviews r
    ) ranked
    where rn = 1
)
select
    p.strategy_id,
    p.proposal_id,
    p.proposal_status,
    p.market_ticker,
    p.city_id,
    p.market_date_local,
    p.side as proposed_side,
    p.market_price as observed_market_price,
    p.target_price,
    p.target_quantity,
    p.fair_prob,
    p.perceived_edge,
    p.strategy_variant,
    p.scenario_label,
    p.thesis,
    p.rationale_summary,
    pb.paper_bet_id,
    pb.status as paper_bet_status,
    pb.side as executed_side,
    pb.limit_price as executed_limit_price,
    pb.quantity as executed_quantity,
    pb.expected_edge,
    pb.realized_pnl,
    coalesce(lr.kalshi_outcome_label, pb.outcome_label) as kalshi_outcome_label,
    lr.lesson_summary,
    lr.review_json
from ops.bet_proposals p
left join ops.paper_bets pb on pb.proposal_id = p.proposal_id
left join latest_reviews lr on lr.paper_bet_id = pb.paper_bet_id;
