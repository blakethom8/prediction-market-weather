-- Sanity checks to prevent time leakage

-- Any forecast used after the market timestamp is a bug.
select *
from features.v_training_rows tr
join core.forecast_snapshots f
  on f.forecast_snapshot_id = tr.latest_forecast_snapshot_id
where f.available_at_utc > tr.ts_utc;
