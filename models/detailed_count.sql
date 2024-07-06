-- Define the model configuration
{{ config(
    materialized='table'
) }}

-- source data input
with source_data as (
    select
        session_id,
        PARSE_DATE('%Y%m%d', event_date) as event_date,
        EXTRACT(YEAR FROM PARSE_DATE('%Y%m%d', event_date)) as year,
        EXTRACT(MONTH FROM PARSE_DATE('%Y%m%d', event_date)) as month,
        EXTRACT(DAY FROM PARSE_DATE('%Y%m%d', event_date)) as day,
        event_timestamp,
        user_first_touch_timestamp,
        event_previous_timestamp,
        event_name,
        user_pseudo_id,
        device_category,
        country,
        region,
        city,
        medium,
        source,
        name  
    from {{ ref('staging_sessions') }}
),

session_durations as (
    select
        event_date,
        user_pseudo_id,
        TIMESTAMP_DIFF(MAX(TIMESTAMP_MICROS(event_timestamp)), MIN(TIMESTAMP_MICROS(event_timestamp)), SECOND) AS session_duration
    from source_data
    group by event_date, user_pseudo_id
),

-- aggregated data
aggregated_data as (
    select
        src.event_date,
        src.year,
        src.month,
        src.day,
        src.device_category,
        src.country,
        src.region,
        src.city,
        src.medium,
        src.source,
        src.name,
        count(distinct (session_id)) as total_sessions,
        count(distinct src.user_pseudo_id) as total_users,
        count(distinct if(TIMESTAMP_MICROS(src.user_first_touch_timestamp) = TIMESTAMP_MICROS(src.event_timestamp), src.user_pseudo_id, null)) as total_new_users,
        count(if(CAST(src.event_name AS STRING) = 'page_view', 1, null)) as total_page_views,
        count(if(CAST(src.event_name AS STRING) = 'view_search_results', 1, null)) as total_sessions_with_search,
        AVG(sd.session_duration) as session_avg_duration
    from source_data src
    left join session_durations sd on sd.event_date = src.event_date and sd.user_pseudo_id = src.user_pseudo_id
    group by year, month, day, event_date, device_category, country, region, city, medium, source, name
)

-- Final select to aggregate all metrics by year, month, day, date, device_category, country, region, city, medium, source, and name
select
    year,
    month,
    day,
    event_date,
    device_category,
    country,
    region,
    city,
    medium,
    source,
    name,
    COALESCE(total_sessions, 0) as total_sessions,
    COALESCE(total_users, 0) as total_users,
    COALESCE(total_new_users, 0) as total_new_users,
    COALESCE(total_page_views, 0) as total_page_views,
    COALESCE(total_sessions_with_search, 0) as total_sessions_with_search,
    COALESCE(session_avg_duration, 0) as session_avg_duration
from aggregated_data
