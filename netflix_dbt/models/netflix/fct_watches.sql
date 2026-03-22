{{ config(materialized='table') }}

with watches as (
    SELECT * from {{ source('netflix_dataset', 'fact_watches') }}
), 

users as (
    SELECT * from {{ source('netflix_dataset', 'dim_users') }}
), 
content as (
    SELECT * from {{ source('netflix_dataset', 'dim_content') }}
)

select
    w.watch_id,
    w.timestamp,
    w.total_duration_min,
    w.rating,
    w.is_rewatch,
    u.country,
    u.age_group,
    c.content_title,
    c.content_type,
    c.genre
from watches w
left join users u on w.user_id = u.user_id
left join content c on w.content_id = c.content_id 

