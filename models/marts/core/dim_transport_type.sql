{{
    config(
        materialized='external',
        location='data/marts/core/dim_transport_type.parquet',
        format='parquet'
    )
}}

with transport_types as (
    select distinct
        transport_type
    from {{ ref('stg_transport_usage') }}
    where transport_type is not null
),

transport_type_dimension as (
    select
        -- Generate a surrogate key for the transport type
        md5(transport_type) as transport_type_key,
        transport_type,
        -- Create a friendly display name
        case transport_type
            when 'cars' then 'Cars'
            when 'light_commercial_vehicles' then 'Light Commercial Vehicles'
            when 'heavy_goods_vehicles' then 'Heavy Goods Vehicles'
            when 'all_motor_vehicles' then 'All Motor Vehicles'
            when 'tfl_tube' then 'TfL Tube'
            when 'tfl_bus' then 'TfL Bus'
            when 'national_rail' then 'National Rail'
            when 'national_rail_noCR' then 'National Rail (excluding Crossrail)'
            else replace(transport_type, '_', ' ')
        end as transport_type_display_name,
        -- Categorize by transport mode
        case
            when transport_type in ('cars', 'light_commercial_vehicles', 'heavy_goods_vehicles', 'all_motor_vehicles') then 'Road'
            when transport_type in ('tfl_tube', 'national_rail', 'national_rail_noCR') then 'Rail'
            when transport_type in ('tfl_bus') then 'Bus'
            else 'Other'
        end as transport_mode,
        -- Categorize by public vs private transport
        case
            when transport_type in ('tfl_tube', 'tfl_bus', 'national_rail', 'national_rail_noCR') then 'Public'
            when transport_type in ('cars', 'light_commercial_vehicles', 'heavy_goods_vehicles', 'all_motor_vehicles') then 'Private/Commercial'
            else 'Other'
        end as transport_category,
        -- Flag for London-specific transport
        case
            when transport_type like 'tfl_%' then true
            else false
        end as is_london_specific,
        -- Flag for commercial/freight transport
        case
            when transport_type in ('light_commercial_vehicles', 'heavy_goods_vehicles') then true
            else false
        end as is_commercial_freight,
        -- Flag for aggregate metrics
        case
            when transport_type in ('all_motor_vehicles') then true
            else false
        end as is_aggregate_metric
    from transport_types
)

select * from transport_type_dimension
order by transport_mode, transport_type

