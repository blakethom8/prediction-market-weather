create table if not exists ops.strategy_sessions (
    strategy_id varchar primary key,
    created_at_utc timestamp,
    strategy_date_local date,
    status varchar,
    focus_cities_json json,
    thesis varchar,
    selection_framework_json json,
    notes_json json
);

create table if not exists ops.strategy_market_board (
    board_entry_id varchar primary key,
    strategy_id varchar,
    market_ticker varchar,
    captured_at_utc timestamp,
    city_id varchar,
    market_date_local date,
    forecast_snapshot_id varchar,
    settlement_source varchar,
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

create table if not exists ops.paper_bets (
    paper_bet_id varchar primary key,
    strategy_id varchar,
    decision_id varchar,
    market_ticker varchar,
    created_at_utc timestamp,
    status varchar,
    side varchar,
    limit_price double,
    quantity double,
    notional_dollars double,
    rationale_summary varchar,
    forecast_snapshot_id varchar,
    realized_pnl double,
    outcome_label varchar,
    closed_at_utc timestamp,
    review_json json
);
