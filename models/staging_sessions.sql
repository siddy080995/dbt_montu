-- Configuration
{{ config(
  materialized='table', 
  cluster_by=['session_id']
) }}


-- Get the data from source data
with stg_source as (
    select 
    *
    from {{ source('ga4_obfuscated_sample_ecommerce', 'events_*') }}
),
-- select relevant columns from source data
source_data as (
    select 
        user_pseudo_id,
        PARSE_DATE('%Y%m%d', event_date) as event_date,
        event_timestamp,
        event_previous_timestamp,
        cast(event_name AS STRING) AS event_name,
        user_first_touch_timestamp,
        case
            when lower(device.category) in ('desktop', 'mobile', 'tablet') then lower(device.category)
            else 'unknown'
        end as device_category,
        geo.country,
        geo.region,
        geo.city,
        traffic_source.medium,
        traffic_source.source,
        traffic_source.name,
        concat(user_pseudo_id, event_timestamp) as session_id  -- unique key session_id by concatenating user_pseudo_id and event_timestamp
    from stg_source
    where user_pseudo_id is not null  -- filtering out rows with null user_pseudo_id
        and event_timestamp is not null  -- filtering out rows with null event_timestamp
)
-- Displaying the source data
select * from source_data


