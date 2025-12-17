{{
    config(
        materialized='external',
        location='data/marts/core/fct_retail_prices.parquet',
        format='parquet'
    )
}}

with staging as (
    select * from {{ ref('stg_retail_prices') }}
),

date_dim as (
    select * from {{ ref('dim_date') }}
),

fact_table as (
    select
        s.retail_price_key,
        s.capture_date as date_key,
        s.supermarket_name,
        s.product_name,
        s.category_name,
        s.price_gbp,
        s.price_unit_gbp,
        s.unit,
        s.is_own_brand,
        -- Join to date dimension for additional context
        d.year,
        d.month,
        d.quarter,
        d.is_weekend
    from staging s
    left join date_dim d on s.capture_date = d.date_key
)

select * from fact_table

