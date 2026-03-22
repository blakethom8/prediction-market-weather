create or replace view features.v_settlement_provenance as
select
    settlement_id,
    source,
    case
        when source = 'nws-cli' then 'official_truth'
        when source = 'kalshi-implied' then 'implied_truth'
        else 'other_truth'
    end as source_class,
    station_id,
    city_id,
    market_date_local,
    observed_high_temp_f,
    observed_low_temp_f,
    observed_precip_in,
    report_published_at_utc,
    is_final
from core.settlement_observations;

create or replace view features.v_settlement_source_comparison as
with official as (
    select *
    from features.v_settlement_provenance
    where source_class = 'official_truth' and is_final = true
), implied as (
    select *
    from features.v_settlement_provenance
    where source_class = 'implied_truth' and is_final = true
)
select
    coalesce(o.city_id, i.city_id) as city_id,
    coalesce(o.station_id, i.station_id) as station_id,
    coalesce(o.market_date_local, i.market_date_local) as market_date_local,
    o.settlement_id as official_settlement_id,
    o.observed_high_temp_f as official_high_temp_f,
    o.report_published_at_utc as official_published_at_utc,
    i.settlement_id as implied_settlement_id,
    i.observed_high_temp_f as implied_high_temp_f,
    i.report_published_at_utc as implied_published_at_utc,
    case
        when o.observed_high_temp_f is not null and i.observed_high_temp_f is not null
            then i.observed_high_temp_f - o.observed_high_temp_f
        else null
    end as implied_minus_official_high_f
from official o
full outer join implied i
    on o.station_id = i.station_id
   and o.market_date_local = i.market_date_local;
