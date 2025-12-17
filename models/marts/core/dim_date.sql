{{
    config(
        materialized='external',
        location='data/marts/core/dim_date.parquet',
        format='parquet'
    )
}}

with all_dates as (
    -- Get all unique dates from transport usage
    select distinct usage_date as date_value
    from {{ ref('stg_transport_usage') }}
    
    union
    
    -- Get all unique dates from retail prices
    select distinct capture_date as date_value
    from {{ ref('stg_retail_prices') }}
),

date_dimension as (
    select
        date_value as date_key,
        date_value,
        extract(year from date_value) as year,
        extract(month from date_value) as month,
        extract(day from date_value) as day,
        extract(quarter from date_value) as quarter,
        extract(dayofweek from date_value) as day_of_week,
        extract(dayofyear from date_value) as day_of_year,
        extract(week from date_value) as week_of_year,
        case 
            when extract(dayofweek from date_value) in (0, 6) then true 
            else false 
        end as is_weekend,
        case extract(month from date_value)
            when 1 then 'January'
            when 2 then 'February'
            when 3 then 'March'
            when 4 then 'April'
            when 5 then 'May'
            when 6 then 'June'
            when 7 then 'July'
            when 8 then 'August'
            when 9 then 'September'
            when 10 then 'October'
            when 11 then 'November'
            when 12 then 'December'
        end as month_name,
        case extract(dayofweek from date_value)
            when 0 then 'Sunday'
            when 1 then 'Monday'
            when 2 then 'Tuesday'
            when 3 then 'Wednesday'
            when 4 then 'Thursday'
            when 5 then 'Friday'
            when 6 then 'Saturday'
        end as day_name
    from all_dates
)

select * from date_dimension
order by date_value

