{{
    config(
        materialized='external',
        location='data/marts/core/fct_transport_usage.parquet',
        format='parquet'
    )
}}

with staging as (
    select * from {{ ref('stg_transport_usage') }}
),

date_dim as (
    select * from {{ ref('dim_date') }}
),

fact_table as (
    select
        s.transport_usage_key,
        s.usage_date as date_key,
        s.transport_type,
        s.usage_value,
        -- Add derived metrics
        case 
            when s.usage_value > 1.0 then 'Above Baseline'
            when s.usage_value = 1.0 then 'At Baseline'
            else 'Below Baseline'
        end as usage_category,
        (s.usage_value - 1.0) * 100 as percent_change_from_baseline,
        -- Join to date dimension for additional context
        d.year,
        d.month,
        d.quarter,
        d.is_weekend
    from staging s
    left join date_dim d on s.usage_date = d.date_key
)

select * from fact_table

