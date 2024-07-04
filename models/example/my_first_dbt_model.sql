
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

with source_data as (

    select * 
    from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20210131` 

)
,

-- Total users
total_users as (

    select count(distinct user_pseudo_id) 
    from source_data
)

-- Total sessions
,
 total_sessions as (
     select count(distinct concat(user_pseudo_id, cast(event_timestamp as string))) 
    from source_data
),

-- Calculate total new users
total_new_users as (
    select count(distinct user_pseudo_id) 
    from source_data
    where user_first_touch_timestamp = event_timestamp
)
,
total_page_views as (
    select count(*)
    from source_data
    where event_name = 'page_view'
),
search_sessions as (
    select
        count(*)
    from source_data
    where event_name = 'view_search_results'
)

select * from
search_sessions
/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
