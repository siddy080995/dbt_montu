-- models/example/unique_sessions.sql
{{ config(
    materialized='table'
) }}

with source_data as (
    select 
        event_date,
        event_timestamp,
        event_name,
        user_pseudo_id,
        user_first_touch_timestamp,
        device.category as device_category,
        geo.country,
        geo.region,
        geo.city,
        traffic_source.medium,
        traffic_source.source,
        traffic_source.name,
        -- Derive session_id by concatenating user_pseudo_id and event_timestamp
        concat(user_pseudo_id, cast(event_timestamp as string)) as session_id
    from {{ source('ga4_obfuscated_sample_ecommerce', 'events_20210131') }}
)

select 
    session_id,
    user_pseudo_id,
    event_date,
    event_timestamp,
    event_name,
    user_first_touch_timestamp,
    device_category,
    country,
    region,
    city,
    medium,
    source,
    name
from source_data
