

-- Define the model configuration
{{ config(
    materialized='table'
) }}

-- source data input
with source_data as (
    select
        PARSE_DATE('%Y%m%d', event_date) as event_date,
        event_timestamp,
        user_first_touch_timestamp,
        event_previous_timestamp,
        event_name,
        user_pseudo_id,
        device_category,
        country,
        name  -- Replaced 'campaign' with 'name'
    from {{ ref('unique_sessions') }}
),

-- total sessions
total_sessions as (
    select
        event_date,
        country,
        name,
        device_category,
        count(distinct concat(user_pseudo_id, cast(event_timestamp as string))) as total_sessions
    from source_data
    group by event_date, country, name, device_category
),

-- total users
total_users as (
    select
        event_date,
        country,
        name,
        device_category,
        count(distinct user_pseudo_id) as total_users
    from source_data
    group by event_date, country, name, device_category
),

-- total new users (first visit)
total_new_users as (
    select
        event_date,
        country,
        name,
        device_category,
        count(distinct user_pseudo_id) as total_new_users
    from source_data
    where user_first_touch_timestamp = event_timestamp
    group by event_date, country, name, device_category
),

-- total page views
total_page_views as (
    select
        event_date,
        country,
        name,
        device_category,
        count(*) as total_page_views
    from source_data
    where CAST(event_name AS STRING) = 'page_view'
    group by event_date, country, name, device_category
),

-- total sessions with search
search_sessions as (
    select
        event_date,
        country,
        name,
        device_category,
        count(*) as total_sessions_with_search
    from source_data
    where CAST(event_name AS STRING) = 'view_search_results'
    group by event_date, country, name, device_category
)

-- Final select to aggregate all metrics by date, country, name and device category
select
    ts.event_date,
    ts.country,
    ts.name,
    ts.device_category,
    COALESCE(ts.total_sessions, 0) as total_sessions,
    COALESCE(tu.total_users, 0) as total_users,
    COALESCE(tnu.total_new_users, 0) as total_new_users,
    COALESCE(tpv.total_page_views, 0) as total_page_views,
    COALESCE(ss.total_sessions_with_search, 0) as total_sessions_with_search
from total_sessions ts
left join total_users tu on ts.event_date = tu.event_date
    and ts.country = tu.country
    and ts.name = tu.name
    and ts.device_category = tu.device_category
left join total_new_users tnu on ts.event_date = tnu.event_date
    and ts.country = tnu.country
    and ts.name = tnu.name
    and ts.device_category = tnu.device_category
left join total_page_views tpv on ts.event_date = tpv.event_date
    and ts.country = tpv.country
    and ts.name = tpv.name
    and ts.device_category = tpv.device_category
left join search_sessions ss on ts.event_date = ss.event_date
    and ts.country = ss.country
    and ts.name = ss.name
    and ts.device_category = ss.device_category
