

-- Define the model configuration
{{ config(
    materialized='table'
) }}

-- getting source data
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
        name  
    from {{ ref('staging_sessions') }}
),

-- total sessions
total_sessions as (
    select
        event_date,
        count(distinct concat(user_pseudo_id, cast(event_timestamp as string))) as total_sessions
    from source_data
    group by event_date
),

-- total users
total_users as (
    select
        event_date,
        count(distinct user_pseudo_id) as total_users
    from source_data
    group by event_date
),

-- total new users (first visit)
total_new_users as (
    select
        event_date,
        count(distinct user_pseudo_id) as total_new_users
    from source_data
    where TIMESTAMP_MICROS(user_first_touch_timestamp) = TIMESTAMP_MICROS(event_timestamp)
    group by event_date
),

-- total page views
total_page_views as (
    select
        event_date,
        count(*) as total_page_views
    from source_data
    where CAST(event_name AS STRING) in ('page_view') 
    group by event_date
),

-- total sessions with search
search_sessions as (
    select
        event_date,
        count(*) as total_sessions_with_search
    from source_data
    where CAST(event_name AS STRING) = 'view_search_results'
    group by event_date
)

-- Final select to aggregate all metrics by date
select
    ts.event_date,
    ts.total_sessions,
    tu.total_users,
    tnu.total_new_users,
    tpv.total_page_views,
    ss.total_sessions_with_search
from total_sessions ts
left join total_users tu on ts.event_date = tu.event_date
left join total_new_users tnu on ts.event_date = tnu.event_date
left join total_page_views tpv on ts.event_date = tpv.event_date
left join search_sessions ss on ts.event_date = ss.event_date

