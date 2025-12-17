{{
    config(
        materialized='external',
        location='data/marts/core/dim_supermarket.parquet',
        format='parquet'
    )
}}

with supermarkets as (
    select distinct
        supermarket_name
    from {{ ref('stg_retail_prices') }}
    where supermarket_name is not null
),

supermarket_dimension as (
    select
        -- Generate a surrogate key for the supermarket
        md5(supermarket_name) as supermarket_key,
        supermarket_name,
        -- Derive supermarket type based on naming patterns
        case
            when lower(supermarket_name) like '%express%' then 'Express/Convenience'
            when lower(supermarket_name) like '%local%' then 'Express/Convenience'
            when lower(supermarket_name) like '%metro%' then 'Express/Convenience'
            when lower(supermarket_name) like '%extra%' then 'Hypermarket'
            when lower(supermarket_name) like '%superstore%' then 'Hypermarket'
            else 'Standard Supermarket'
        end as store_format,
        -- Categorize by market positioning
        case
            when lower(supermarket_name) in ('aldi', 'lidl') then 'Discount'
            when lower(supermarket_name) in ('waitrose', 'marks & spencer', 'm&s') then 'Premium'
            else 'Mid-Market'
        end as market_segment,
        -- Flag for own-brand focus retailers
        case
            when lower(supermarket_name) in ('aldi', 'lidl') then true
            else false
        end as is_discounter
    from supermarkets
)

select * from supermarket_dimension
order by supermarket_name

