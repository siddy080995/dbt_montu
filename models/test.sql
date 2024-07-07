{{ config(
  materialized='incremental',
  unique_key='session_id',
  incremental_strategy='insert_overwrite',
  partition_by={
    "field": "event_date",
    "data_type": "date"
  }
) }}

with source_data as (
    select 
        PARSE_DATE('%Y%m%d', event_date) as event_date,
        event_timestamp,
        event_previous_timestamp,
        cast(event_name AS STRING) AS event_name,
        user_pseudo_id,
        user_first_touch_timestamp,
        device.category as device_category,
        geo.country,
        geo.region,
        geo.city,
        traffic_source.medium,
        traffic_source.source,
        traffic_source.name,
        concat(user_pseudo_id, cast(event_timestamp as string)) as session_id  -- unique key session_id by concatenating user_pseudo_id and event_timestamp
    from {{ source('ga4_obfuscated_sample_ecommerce', 'events_*') }}
)

{% if is_incremental() %}

select 
    session_id,
    user_pseudo_id,
    event_date,
    event_timestamp,
    event_previous_timestamp,
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
where TIMESTAMP_MICROS(event_timestamp) > (
    select max(TIMESTAMP_MICROS(event_timestamp)) 
    from {{ this }}
)

{% else %}

select 
    session_id,
    user_pseudo_id,
    event_date,
    event_timestamp,
    event_previous_timestamp,
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

{% endif %}
