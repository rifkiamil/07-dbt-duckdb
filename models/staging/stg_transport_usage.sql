{{
    config(
        materialized='external',
        location='data/staging/stg_transport_usage.parquet',
        format='parquet'
    )
}}

with source as (
    select * from read_csv_auto('raw/transport-uk/transport-use-statistics.csv')
),

cleaned as (
    select
        cast(date as date) as usage_date,
        transport_type,
        cast(value as double) as usage_value,
        -- Add a surrogate key using hash
        md5(date || transport_type) as transport_usage_key
    from source
)

select * from cleaned

