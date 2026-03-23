create table if not exists ops.strategy_sessions (
    strategy_id varchar primary key,
    created_at_utc timestamp,
    strategy_date_local date,
    status varchar,
    approval_status varchar,
    approved_at_utc timestamp,
    last_reviewed_at_utc timestamp,
    approval_notes_json json,
    focus_cities_json json,
    research_focus_cities_json json,
    board_scope varchar,
    board_filters_json json,
    board_generated_at_utc timestamp,
    board_market_count integer,
    board_city_count integer,
    thesis varchar,
    selection_framework_json json,
    strategy_variant varchar,
    scenario_label varchar,
    session_context_json json,
    notes_json json
);

alter table ops.strategy_sessions add column if not exists last_reviewed_at_utc timestamp;
alter table ops.strategy_sessions add column if not exists research_focus_cities_json json;
alter table ops.strategy_sessions add column if not exists board_scope varchar;
alter table ops.strategy_sessions add column if not exists board_filters_json json;
alter table ops.strategy_sessions add column if not exists board_generated_at_utc timestamp;
alter table ops.strategy_sessions add column if not exists board_market_count integer;
alter table ops.strategy_sessions add column if not exists board_city_count integer;
alter table ops.strategy_sessions add column if not exists strategy_variant varchar;
alter table ops.strategy_sessions add column if not exists scenario_label varchar;
alter table ops.strategy_sessions add column if not exists session_context_json json;

create table if not exists ops.strategy_market_board (
    board_entry_id varchar primary key,
    strategy_id varchar,
    market_ticker varchar,
    market_title varchar,
    captured_at_utc timestamp,
    city_id varchar,
    market_date_local date,
    forecast_snapshot_id varchar,
    minutes_to_close integer,
    price_yes_mid double,
    price_yes_ask double,
    price_yes_bid double,
    fair_prob double,
    edge_vs_mid double,
    edge_vs_ask double,
    candidate_rank integer,
    candidate_bucket varchar,
    board_notes_json json
);

alter table ops.strategy_market_board add column if not exists market_title varchar;
alter table ops.strategy_market_board add column if not exists minutes_to_close integer;
alter table ops.strategy_market_board drop column if exists settlement_source;

create table if not exists ops.strategy_review_events (
    strategy_review_id varchar primary key,
    strategy_id varchar,
    reviewed_at_utc timestamp,
    actor varchar,
    decision varchar,
    resulting_approval_status varchar,
    notes_json json
);

create table if not exists ops.bet_proposals (
    proposal_id varchar primary key,
    strategy_id varchar,
    board_entry_id varchar,
    market_ticker varchar,
    city_id varchar,
    market_date_local date,
    proposed_at_utc timestamp,
    proposal_status varchar,
    side varchar,
    market_price double,
    target_price double,
    target_quantity double,
    fair_prob double,
    perceived_edge double,
    candidate_rank integer,
    candidate_bucket varchar,
    forecast_snapshot_id varchar,
    strategy_variant varchar,
    scenario_label varchar,
    thesis varchar,
    rationale_summary varchar,
    rationale_json json,
    context_json json,
    linked_paper_bet_id varchar
);

create table if not exists ops.bet_proposal_events (
    proposal_event_id varchar primary key,
    proposal_id varchar,
    strategy_id varchar,
    event_at_utc timestamp,
    actor varchar,
    decision varchar,
    resulting_status varchar,
    notes_json json
);

create table if not exists ops.paper_bets (
    paper_bet_id varchar primary key,
    strategy_id varchar,
    proposal_id varchar,
    decision_id varchar,
    market_ticker varchar,
    created_at_utc timestamp,
    status varchar,
    side varchar,
    limit_price double,
    quantity double,
    notional_dollars double,
    expected_edge double,
    strategy_variant varchar,
    scenario_label varchar,
    thesis_at_entry varchar,
    rationale_summary varchar,
    rationale_json json,
    forecast_snapshot_id varchar,
    realized_pnl double,
    outcome_label varchar,
    closed_at_utc timestamp,
    review_json json
);

alter table ops.paper_bets add column if not exists proposal_id varchar;
alter table ops.paper_bets add column if not exists expected_edge double;
alter table ops.paper_bets add column if not exists strategy_variant varchar;
alter table ops.paper_bets add column if not exists scenario_label varchar;
alter table ops.paper_bets add column if not exists thesis_at_entry varchar;
alter table ops.paper_bets add column if not exists rationale_json json;

create table if not exists ops.paper_bet_reviews (
    review_id varchar primary key,
    paper_bet_id varchar,
    proposal_id varchar,
    strategy_id varchar,
    reviewed_at_utc timestamp,
    kalshi_outcome_label varchar,
    realized_pnl double,
    lesson_summary varchar,
    review_json json
);
