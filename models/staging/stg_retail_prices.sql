{{
    config(
        materialized='external',
        location='data/staging/stg_retail_prices.parquet',
        format='parquet'
    )
}}

with source as (
    select * from read_parquet('raw/time-series-uk-retail-supermarket-price-data/base_retail_gb_snappy.parquet')
),

cleaned as (
    select
        cast(capture_date as date) as capture_date,
        supermarket_name,
        product_name,
        category_name,
        cast(price_gbp as decimal(10,4)) as price_gbp,
        cast(price_unit_gbp as decimal(10,4)) as price_unit_gbp,
        unit,
        is_own_brand,
        -- Add a surrogate key using hash
        md5(cast(capture_date as varchar) || supermarket_name || product_name) as retail_price_key
    from source
)

select * from cleaned

