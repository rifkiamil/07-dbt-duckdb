{{
    config(
        materialized='external',
        location='data/marts/core/dim_product.parquet',
        format='parquet'
    )
}}

with products as (
    select distinct
        product_name,
        category_name,
        unit,
        is_own_brand
    from {{ ref('stg_retail_prices') }}
    where product_name is not null
),

product_dimension as (
    select
        -- Generate a surrogate key for the product
        md5(product_name || coalesce(category_name, '')) as product_key,
        product_name,
        category_name,
        unit,
        is_own_brand,
        -- Derive high-level category grouping
        case
            when lower(category_name) like '%fruit%' or lower(category_name) like '%vegetable%' or lower(category_name) like '%produce%' then 'Fresh Produce'
            when lower(category_name) like '%meat%' or lower(category_name) like '%poultry%' or lower(category_name) like '%fish%' then 'Protein'
            when lower(category_name) like '%dairy%' or lower(category_name) like '%milk%' or lower(category_name) like '%cheese%' then 'Dairy'
            when lower(category_name) like '%bread%' or lower(category_name) like '%bakery%' then 'Bakery'
            when lower(category_name) like '%frozen%' then 'Frozen Foods'
            when lower(category_name) like '%drink%' or lower(category_name) like '%beverage%' then 'Beverages'
            when lower(category_name) like '%snack%' or lower(category_name) like '%confection%' then 'Snacks & Confectionery'
            when lower(category_name) like '%household%' or lower(category_name) like '%cleaning%' then 'Household'
            when lower(category_name) like '%personal%' or lower(category_name) like '%health%' then 'Health & Personal Care'
            else 'Other'
        end as category_group,
        -- Flag for fresh/perishable products
        case
            when lower(category_name) like '%fresh%' 
                or lower(category_name) like '%fruit%' 
                or lower(category_name) like '%vegetable%'
                or lower(category_name) like '%dairy%'
                or lower(category_name) like '%meat%'
                or lower(category_name) like '%bakery%'
            then true
            else false
        end as is_perishable,
        -- Standardize unit description
        case
            when lower(unit) like '%kg%' or lower(unit) like '%kilo%' then 'Weight (kg)'
            when lower(unit) like '%g%' and lower(unit) not like '%kg%' then 'Weight (g)'
            when lower(unit) like '%l%' or lower(unit) like '%litre%' then 'Volume (l)'
            when lower(unit) like '%ml%' then 'Volume (ml)'
            when lower(unit) like '%each%' or lower(unit) like '%unit%' then 'Each'
            when lower(unit) like '%pack%' then 'Pack'
            else 'Other'
        end as unit_type
    from products
)

select * from product_dimension
order by category_name, product_name

