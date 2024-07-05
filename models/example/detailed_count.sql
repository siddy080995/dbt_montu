-- Define the model configuration
{{ config(
    materialized='table'
) }}

-- source data input
with source_data as (
    select
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
        name  -- Replaced 'campaign' with 'name'
    from {{ ref('unique_sessions') }}
),

-- aggregated data
aggregated_data as (
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
        count(distinct concat(user_pseudo_id, cast(event_timestamp as string))) as total_sessions,
        count(distinct user_pseudo_id) as total_users,
        count(distinct if(user_first_touch_timestamp = event_timestamp, user_pseudo_id, null)) as total_new_users,
        count(if(CAST(event_name AS STRING) = 'page_view', 1, null)) as total_page_views,
        count(if(CAST(event_name AS STRING) = 'view_search_results', 1, null)) as total_sessions_with_search
    from source_data
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
    COALESCE(total_sessions_with_search, 0) as total_sessions_with_search
from aggregated_data
