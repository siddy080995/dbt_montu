{{ config(
    materialized='incremental',
    unique_key='session_id'
) }}

with source_data as (
    select * 
    from {{ source('ga4_obfuscated_sample_ecommerce', 'events_20210131') }}
),

sessions as (
    select
        concat(user_pseudo_id, '-', cast(event_timestamp as string)) as session_id,
        user_pseudo_id,
        event_date,
        event_timestamp,
        event_name,
        event_server_timestamp_offset,
        user_first_touch_timestamp,
        geo.country,
        geo.region,
        geo.city

    from
        source_data
)

select * from sessions
