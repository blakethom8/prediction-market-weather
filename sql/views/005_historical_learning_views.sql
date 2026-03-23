create or replace view ops.v_strategy_board_learning_history as
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
),
latest_approval_events as (
    select *
    from (
        select
            e.*,
            row_number() over (
                partition by e.proposal_id
                order by e.event_at_utc desc nulls last, e.proposal_event_id desc
            ) as rn
        from ops.bet_proposal_events e
        where e.resulting_status in ('approved', 'adjustments_requested', 'rejected')
    ) ranked
    where rn = 1
)
select
    smb.strategy_id,
    s.strategy_date_local,
    s.created_at_utc as strategy_created_at_utc,
    s.approval_status as session_approval_status,
    s.thesis as session_thesis,
    s.strategy_variant as session_strategy_variant,
    s.scenario_label as session_scenario_label,
    smb.board_entry_id,
    smb.market_ticker,
    smb.market_title,
    coalesce(p.city_id, smb.city_id, c.city_id) as city_id,
    coalesce(p.market_date_local, smb.market_date_local, c.market_date_local) as market_date_local,
    smb.captured_at_utc,
    smb.minutes_to_close,
    case
        when smb.minutes_to_close is null then null
        when smb.minutes_to_close < 120 then '<2h'
        when smb.minutes_to_close < 360 then '2-6h'
        when smb.minutes_to_close < 720 then '6-12h'
        else '12h+'
    end as time_to_close_bucket,
    smb.price_yes_mid,
    smb.price_yes_ask,
    smb.price_yes_bid,
    smb.fair_prob,
    smb.edge_vs_mid,
    smb.edge_vs_ask,
    smb.candidate_rank,
    smb.candidate_bucket,
    c.threshold_low_f,
    c.threshold_high_f,
    p.proposal_id,
    p.proposed_at_utc,
    p.proposal_status as proposal_final_status,
    case
        when p.proposal_id is null then 'not_proposed'
        when lae.resulting_status is not null then lae.resulting_status
        when p.proposal_status in ('approved', 'adjustments_requested', 'rejected') then p.proposal_status
        when s.approval_status in ('approved', 'adjustments_requested', 'rejected') then s.approval_status
        else 'pending_review'
    end as approval_outcome,
    lae.event_at_utc as approval_reviewed_at_utc,
    lae.decision as approval_decision,
    lae.notes_json as approval_notes_json,
    p.side as proposed_side,
    p.market_price as observed_market_price,
    p.target_price,
    p.target_quantity,
    p.perceived_edge,
    coalesce(p.strategy_variant, s.strategy_variant, 'baseline') as strategy_variant,
    coalesce(p.scenario_label, s.scenario_label, 'live') as scenario_label,
    p.thesis,
    p.rationale_summary,
    pb.paper_bet_id,
    pb.created_at_utc as paper_bet_created_at_utc,
    pb.status as paper_bet_status,
    pb.side as executed_side,
    pb.limit_price as executed_limit_price,
    pb.quantity as executed_quantity,
    pb.notional_dollars,
    pb.expected_edge,
    pb.realized_pnl,
    pb.closed_at_utc,
    coalesce(lr.kalshi_outcome_label, pb.outcome_label) as kalshi_outcome_label,
    lr.lesson_summary,
    lr.review_json,
    case when p.proposal_id is not null then true else false end as proposed_flag,
    case when pb.paper_bet_id is not null then true else false end as converted_flag,
    case
        when pb.status = 'closed' and coalesce(pb.realized_pnl, 0.0) > 0 then 1
        when pb.status = 'closed' and coalesce(pb.realized_pnl, 0.0) < 0 then 0
        else null
    end as win_flag
from ops.strategy_market_board smb
left join ops.strategy_sessions s on s.strategy_id = smb.strategy_id
left join ops.bet_proposals p on p.board_entry_id = smb.board_entry_id
left join latest_approval_events lae on lae.proposal_id = p.proposal_id
left join ops.paper_bets pb on pb.proposal_id = p.proposal_id
left join latest_reviews lr on lr.paper_bet_id = pb.paper_bet_id
left join core.weather_contracts c on c.market_ticker = smb.market_ticker;

create or replace view ops.v_paper_bet_history as
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
),
board_context as (
    select
        proposal_id,
        board_entry_id,
        market_title,
        city_id,
        market_date_local,
        candidate_rank,
        candidate_bucket,
        approval_outcome,
        minutes_to_close,
        time_to_close_bucket,
        strategy_variant,
        scenario_label,
        threshold_low_f,
        threshold_high_f
    from ops.v_strategy_board_learning_history
    where proposal_id is not null
)
select
    pb.paper_bet_id,
    pb.strategy_id,
    s.strategy_date_local,
    s.created_at_utc as strategy_created_at_utc,
    pb.proposal_id,
    bc.board_entry_id,
    pb.market_ticker,
    coalesce(bc.market_title, c.title) as market_title,
    coalesce(bc.city_id, c.city_id) as city_id,
    coalesce(bc.market_date_local, c.market_date_local) as market_date_local,
    pb.created_at_utc,
    cast(pb.created_at_utc as date) as created_date_utc,
    pb.closed_at_utc,
    cast(pb.closed_at_utc as date) as closed_date_utc,
    pb.status,
    pb.side,
    pb.limit_price,
    pb.quantity,
    pb.notional_dollars,
    pb.expected_edge,
    pb.realized_pnl,
    coalesce(lr.kalshi_outcome_label, pb.outcome_label) as outcome_label,
    lr.lesson_summary,
    lr.review_json,
    coalesce(pb.strategy_variant, bc.strategy_variant, s.strategy_variant, 'baseline') as strategy_variant,
    coalesce(pb.scenario_label, bc.scenario_label, s.scenario_label, 'live') as scenario_label,
    pb.thesis_at_entry,
    coalesce(bc.candidate_bucket, 'manual') as candidate_bucket,
    bc.candidate_rank,
    bc.approval_outcome,
    bc.minutes_to_close,
    bc.time_to_close_bucket,
    coalesce(bc.threshold_low_f, c.threshold_low_f) as threshold_low_f,
    coalesce(bc.threshold_high_f, c.threshold_high_f) as threshold_high_f,
    case
        when pb.status = 'closed' and coalesce(pb.realized_pnl, 0.0) > 0 then 1
        when pb.status = 'closed' and coalesce(pb.realized_pnl, 0.0) < 0 then 0
        else null
    end as win_flag
from ops.paper_bets pb
left join ops.strategy_sessions s on s.strategy_id = pb.strategy_id
left join board_context bc on bc.proposal_id = pb.proposal_id
left join latest_reviews lr on lr.paper_bet_id = pb.paper_bet_id
left join core.weather_contracts c on c.market_ticker = pb.market_ticker;

create or replace view ops.v_strategy_session_learning as
with board_stats as (
    select
        strategy_id,
        count(*) as board_row_count,
        count(*) filter (where candidate_bucket = 'priority') as priority_candidate_count,
        count(*) filter (where candidate_bucket = 'watch') as watch_candidate_count,
        count(*) filter (where candidate_bucket = 'pass') as pass_candidate_count,
        count(*) filter (where proposed_flag) as proposal_count,
        count(*) filter (where approval_outcome = 'approved') as approved_count,
        count(*) filter (where approval_outcome = 'adjustments_requested') as adjusted_count,
        count(*) filter (where approval_outcome = 'rejected') as rejected_count,
        count(*) filter (where converted_flag) as converted_count
    from ops.v_strategy_board_learning_history
    group by strategy_id
),
paper_stats as (
    select
        strategy_id,
        count(*) filter (where status = 'open') as open_paper_bets,
        count(*) filter (where status = 'closed') as closed_paper_bets,
        coalesce(sum(notional_dollars) filter (where status = 'open'), 0.0) as open_notional,
        coalesce(sum(realized_pnl) filter (where status = 'closed'), 0.0) as closed_realized_pnl,
        avg(expected_edge) filter (where status = 'closed' and expected_edge is not null) as avg_closed_expected_edge,
        avg(realized_pnl) filter (where status = 'closed' and realized_pnl is not null) as avg_closed_realized_pnl,
        avg(win_flag) filter (where status = 'closed' and win_flag is not null) as win_rate,
        max(closed_at_utc) filter (where status = 'closed') as last_closed_at_utc
    from ops.v_paper_bet_history
    group by strategy_id
),
latest_lessons as (
    select *
    from (
        select
            strategy_id,
            lesson_summary,
            closed_at_utc,
            row_number() over (
                partition by strategy_id
                order by closed_at_utc desc nulls last, paper_bet_id desc
            ) as rn
        from ops.v_paper_bet_history
        where status = 'closed' and lesson_summary is not null
    ) ranked
    where rn = 1
),
latest_reviews as (
    select *
    from (
        select
            r.*,
            row_number() over (
                partition by r.strategy_id
                order by r.reviewed_at_utc desc nulls last, r.strategy_review_id desc
            ) as rn
        from ops.strategy_review_events r
    ) ranked
    where rn = 1
)
select
    s.strategy_id,
    s.created_at_utc,
    s.strategy_date_local,
    s.status,
    s.approval_status,
    s.strategy_variant,
    s.scenario_label,
    s.board_market_count,
    s.board_city_count,
    s.thesis,
    coalesce(bs.board_row_count, 0) as board_row_count,
    coalesce(bs.priority_candidate_count, 0) as priority_candidate_count,
    coalesce(bs.watch_candidate_count, 0) as watch_candidate_count,
    coalesce(bs.pass_candidate_count, 0) as pass_candidate_count,
    coalesce(bs.proposal_count, 0) as proposal_count,
    coalesce(bs.approved_count, 0) as approved_count,
    coalesce(bs.adjusted_count, 0) as adjusted_count,
    coalesce(bs.rejected_count, 0) as rejected_count,
    coalesce(bs.converted_count, 0) as converted_count,
    coalesce(ps.open_paper_bets, 0) as open_paper_bets,
    coalesce(ps.closed_paper_bets, 0) as closed_paper_bets,
    coalesce(ps.open_notional, 0.0) as open_notional,
    coalesce(ps.closed_realized_pnl, 0.0) as closed_realized_pnl,
    ps.avg_closed_expected_edge,
    ps.avg_closed_realized_pnl,
    ps.win_rate,
    ps.last_closed_at_utc,
    ll.lesson_summary as latest_lesson,
    lr.reviewed_at_utc as latest_reviewed_at_utc,
    lr.decision as latest_review_decision,
    lr.notes_json as latest_review_notes_json
from ops.strategy_sessions s
left join board_stats bs on bs.strategy_id = s.strategy_id
left join paper_stats ps on ps.strategy_id = s.strategy_id
left join latest_lessons ll on ll.strategy_id = s.strategy_id
left join latest_reviews lr on lr.strategy_id = s.strategy_id;
