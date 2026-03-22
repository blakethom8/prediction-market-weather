create or replace view features.v_city_source_coverage as
with contract_counts as (
    select city_id, count(*) as contract_count
    from core.weather_contracts
    where city_id is not null
    group by city_id
), snapshot_counts as (
    select c.city_id, count(*) as market_snapshot_count
    from core.market_snapshots m
    join core.weather_contracts c on c.market_ticker = m.market_ticker
    where c.city_id is not null
    group by c.city_id
), forecast_counts as (
    select
        city_id,
        count(*) as forecast_snapshot_count,
        count(*) filter (where source = 'open-meteo') as open_meteo_live_count,
        count(*) filter (where source = 'open-meteo-archive') as open_meteo_archive_count,
        count(*) filter (where source not in ('open-meteo', 'open-meteo-archive')) as other_forecast_count
    from core.forecast_snapshots
    where city_id is not null
    group by city_id
), settlement_counts as (
    select
        city_id,
        count(*) filter (where source = 'nws-cli' and is_final = true) as official_settlement_count,
        count(*) filter (where source = 'kalshi-implied' and is_final = true) as implied_settlement_count,
        count(*) filter (where source not in ('nws-cli', 'kalshi-implied') and is_final = true) as other_settlement_count
    from core.settlement_observations
    where city_id is not null
    group by city_id
)
select
    ci.city_id,
    ci.city_name,
    coalesce(cc.contract_count, 0) as contract_count,
    coalesce(sc.market_snapshot_count, 0) as market_snapshot_count,
    coalesce(fc.forecast_snapshot_count, 0) as forecast_snapshot_count,
    coalesce(fc.open_meteo_live_count, 0) as open_meteo_live_count,
    coalesce(fc.open_meteo_archive_count, 0) as open_meteo_archive_count,
    coalesce(fc.other_forecast_count, 0) as other_forecast_count,
    coalesce(se.official_settlement_count, 0) as official_settlement_count,
    coalesce(se.implied_settlement_count, 0) as implied_settlement_count,
    coalesce(se.other_settlement_count, 0) as other_settlement_count,
    case
        when coalesce(se.official_settlement_count, 0) > 0 and coalesce(fc.open_meteo_live_count, 0) > 0 then 'research_ready'
        when coalesce(se.official_settlement_count, 0) > 0 and coalesce(fc.open_meteo_archive_count, 0) > 0 then 'proxy_forecast_only'
        when coalesce(se.implied_settlement_count, 0) > 0 or coalesce(fc.open_meteo_archive_count, 0) > 0 then 'bootstrap_only'
        else 'thin'
    end as readiness_band
from core.cities ci
left join contract_counts cc on cc.city_id = ci.city_id
left join snapshot_counts sc on sc.city_id = ci.city_id
left join forecast_counts fc on fc.city_id = ci.city_id
left join settlement_counts se on se.city_id = ci.city_id
order by contract_count desc, ci.city_id;
