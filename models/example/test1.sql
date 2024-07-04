
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

{{ config(
    materialized='table'
) }}

with source_data as (
    select * 
    from {{ source('ga4_obfuscated_sample_ecommerce', 'events_20210131') }}
),

-- Total users
total_users as (
    select count(distinct user_pseudo_id) as total_users
    from source_data
),

-- Total sessions
total_sessions as (
    select count(distinct concat(user_pseudo_id, cast(event_timestamp as string))) as total_sessions
    from source_data
),

-- Calculate total new users
total_new_users as (
    select count(distinct user_pseudo_id) as total_new_users
    from source_data
    where user_first_touch_timestamp = event_timestamp
),

-- Total page views
total_page_views as (
    select count(*) as total_page_views
    from source_data
    where event_name = 'page_view'
),

-- Total sessions with search
search_sessions as (
    select count(*) as total_sessions_with_search
    from source_data
    where event_name = 'view_search_results'
)

select 
    (select total_users from total_users) as total_users,
    (select total_sessions from total_sessions) as total_sessions,
    (select total_new_users from total_new_users) as total_new_users,
    (select total_page_views from total_page_views) as total_page_views,
    (select total_sessions_with_search from search_sessions) as total_sessions_with_search
