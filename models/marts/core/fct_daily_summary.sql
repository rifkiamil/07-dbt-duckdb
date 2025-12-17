{{
    config(
        materialized='external',
        location='data/marts/core/fct_daily_summary.parquet',
        format='parquet'
    )
}}

{#
    Aggregated fact table combining retail and transport metrics for cross-domain analysis.
    This provides a high-level summary view enabling correlation analysis between
    consumer behavior (retail prices) and mobility patterns (transport usage) during COVID-19.
#}

with retail_facts as (
    select
        date_key,
        count(*) as total_retail_observations,
        count(distinct supermarket_name) as supermarket_count,
        count(distinct category_name) as category_count,
        avg(price_gbp) as avg_price_gbp,
        percentile_cont(0.5) within group (order by price_gbp) as median_price_gbp,
        stddev(price_gbp) as price_stddev_gbp,
        avg(price_unit_gbp) as avg_unit_price_gbp,
        sum(case when is_own_brand then 1 else 0 end)::float / nullif(count(*), 0) as own_brand_ratio
    from {{ ref('fct_retail_prices') }}
    group by date_key
),

transport_facts as (
    select
        date_key,
        count(*) as total_transport_observations,
        count(distinct transport_type) as transport_type_count,
        avg(usage_value) as avg_usage_value,
        avg(percent_change_from_baseline) as avg_pct_change_from_baseline,
        -- Aggregate by usage category
        sum(case when usage_category = 'Above Baseline' then 1 else 0 end) as above_baseline_count,
        sum(case when usage_category = 'Below Baseline' then 1 else 0 end) as below_baseline_count,
        sum(case when usage_category = 'At Baseline' then 1 else 0 end) as at_baseline_count,
        -- Road vs Rail summary
        avg(case when transport_type in ('cars', 'light_commercial_vehicles', 'heavy_goods_vehicles', 'all_motor_vehicles') 
            then usage_value end) as avg_road_usage,
        avg(case when transport_type in ('tfl_tube', 'national_rail', 'national_rail_noCR') 
            then usage_value end) as avg_rail_usage,
        avg(case when transport_type in ('tfl_bus') 
            then usage_value end) as avg_bus_usage
    from {{ ref('fct_transport_usage') }}
    group by date_key
),

date_dim as (
    select * from {{ ref('dim_date') }}
),

daily_summary as (
    select
        d.date_key,
        d.date_value,
        d.year,
        d.month,
        d.month_name,
        d.quarter,
        d.week_of_year,
        d.day_name,
        d.is_weekend,
        -- Generate a surrogate key
        md5(cast(d.date_key as varchar) || 'daily_summary') as daily_summary_key,
        -- Retail metrics
        coalesce(r.total_retail_observations, 0) as retail_observations,
        r.supermarket_count,
        r.category_count,
        r.avg_price_gbp,
        r.median_price_gbp,
        r.price_stddev_gbp,
        r.avg_unit_price_gbp,
        r.own_brand_ratio,
        -- Transport metrics
        coalesce(t.total_transport_observations, 0) as transport_observations,
        t.transport_type_count,
        t.avg_usage_value as avg_transport_usage,
        t.avg_pct_change_from_baseline,
        t.above_baseline_count,
        t.below_baseline_count,
        t.at_baseline_count,
        t.avg_road_usage,
        t.avg_rail_usage,
        t.avg_bus_usage,
        -- Cross-domain indicators
        case
            when r.total_retail_observations > 0 and t.total_transport_observations > 0 then 'Complete'
            when r.total_retail_observations > 0 then 'Retail Only'
            when t.total_transport_observations > 0 then 'Transport Only'
            else 'No Data'
        end as data_completeness,
        -- COVID-19 period classification (approximate UK lockdown periods)
        case
            when d.date_value between '2020-03-23' and '2020-06-15' then 'First Lockdown'
            when d.date_value between '2020-11-05' and '2020-12-02' then 'Second Lockdown'
            when d.date_value between '2021-01-06' and '2021-03-08' then 'Third Lockdown'
            when d.date_value < '2020-03-23' then 'Pre-Pandemic'
            else 'Between Restrictions'
        end as covid_period,
        -- Mobility summary indicator
        case
            when t.avg_usage_value is null then null
            when t.avg_usage_value >= 0.9 then 'Near Normal'
            when t.avg_usage_value >= 0.7 then 'Moderately Reduced'
            when t.avg_usage_value >= 0.5 then 'Significantly Reduced'
            else 'Severely Reduced'
        end as mobility_indicator
    from date_dim d
    left join retail_facts r on d.date_key = r.date_key
    left join transport_facts t on d.date_key = t.date_key
    where r.total_retail_observations > 0 or t.total_transport_observations > 0
)

select * from daily_summary
order by date_key

