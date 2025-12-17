{{
    config(
        materialized='external',
        location='data/staging/stg_combined_daily_metrics.parquet',
        format='parquet'
    )
}}

{#
    Combined staging fact table that joins retail and transport data by date.
    This provides a unified view of daily metrics from both domains for cross-analysis.
#}

with retail_daily as (
    -- Aggregate retail prices to daily level
    select
        capture_date as metric_date,
        count(*) as retail_price_count,
        count(distinct product_name) as unique_products,
        count(distinct supermarket_name) as unique_supermarkets,
        count(distinct category_name) as unique_categories,
        avg(price_gbp) as avg_price_gbp,
        min(price_gbp) as min_price_gbp,
        max(price_gbp) as max_price_gbp,
        avg(price_unit_gbp) as avg_unit_price_gbp,
        sum(case when is_own_brand then 1 else 0 end) as own_brand_count
    from {{ ref('stg_retail_prices') }}
    group by capture_date
),

transport_daily as (
    -- Pivot transport types to get daily summary
    select
        usage_date as metric_date,
        count(*) as transport_observation_count,
        count(distinct transport_type) as transport_types_tracked,
        avg(usage_value) as avg_usage_value,
        min(usage_value) as min_usage_value,
        max(usage_value) as max_usage_value,
        -- Get specific transport type values when available
        max(case when transport_type = 'all_motor_vehicles' then usage_value end) as motor_vehicles_usage,
        max(case when transport_type = 'tfl_tube' then usage_value end) as tfl_tube_usage,
        max(case when transport_type = 'tfl_bus' then usage_value end) as tfl_bus_usage,
        max(case when transport_type = 'national_rail' then usage_value end) as national_rail_usage
    from {{ ref('stg_transport_usage') }}
    group by usage_date
),

combined_metrics as (
    select
        coalesce(r.metric_date, t.metric_date) as metric_date,
        md5(cast(coalesce(r.metric_date, t.metric_date) as varchar)) as combined_metrics_key,
        -- Retail metrics
        r.retail_price_count,
        r.unique_products,
        r.unique_supermarkets,
        r.unique_categories,
        r.avg_price_gbp,
        r.min_price_gbp,
        r.max_price_gbp,
        r.avg_unit_price_gbp,
        r.own_brand_count,
        -- Transport metrics
        t.transport_observation_count,
        t.transport_types_tracked,
        t.avg_usage_value as avg_transport_usage,
        t.min_usage_value as min_transport_usage,
        t.max_usage_value as max_transport_usage,
        t.motor_vehicles_usage,
        t.tfl_tube_usage,
        t.tfl_bus_usage,
        t.national_rail_usage,
        -- Data availability flags
        case when r.metric_date is not null then true else false end as has_retail_data,
        case when t.metric_date is not null then true else false end as has_transport_data,
        case when r.metric_date is not null and t.metric_date is not null then true else false end as has_both_sources
    from retail_daily r
    full outer join transport_daily t on r.metric_date = t.metric_date
)

select * from combined_metrics
order by metric_date

