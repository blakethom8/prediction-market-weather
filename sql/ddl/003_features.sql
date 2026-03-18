-- Feature / observability tables

create schema if not exists features;
create schema if not exists ops;

create table if not exists features.contract_training_rows (
    market_ticker varchar,
    ts_utc timestamp,
    city_id varchar,
    market_date_local date,
    price_yes_mid double,
    price_yes_ask double,
    price_yes_bid double,
    fair_prob double,
    edge_vs_mid double,
    edge_vs_ask double,
    minutes_to_close integer,
    sibling_rank integer,
    sibling_count integer,
    sibling_entropy double,
    latest_forecast_snapshot_id varchar,
    y_resolve_yes integer,
    primary key (market_ticker, ts_utc)
);

create table if not exists ops.pipeline_runs (
    run_id varchar,
    job_name varchar,
    started_at_utc timestamp,
    finished_at_utc timestamp,
    status varchar,
    rows_read bigint,
    rows_written bigint,
    message varchar
);

create table if not exists ops.decision_journal (
    decision_id varchar primary key,
    market_ticker varchar,
    decided_at_utc timestamp,
    signal_version varchar,
    fair_prob double,
    market_mid double,
    tradable_yes_ask double,
    tradable_yes_bid double,
    edge_vs_mid double,
    edge_vs_ask double,
    confidence double,
    action varchar,
    abstain_reason varchar,
    rationale_json json
);

create table if not exists ops.bet_executions (
    execution_id varchar primary key,
    decision_id varchar,
    submitted_at_utc timestamp,
    side varchar,
    limit_price double,
    quantity double,
    fill_status varchar,
    avg_fill_price double,
    fees_paid double,
    slippage double,
    notes varchar
);

create table if not exists ops.bet_reviews (
    review_id varchar primary key,
    decision_id varchar,
    reviewed_at_utc timestamp,
    realized_pnl double,
    outcome_label varchar,
    error_type varchar,
    review_summary varchar,
    lessons_json json
);
