{{ config(materialized='view') }}

with watches as (
    SELECT * from {{ ref('fct_watches') }}
)

SELECT 
    country,
    COUNT(*) AS watch_count, 
    round(AVG(rating), 2) AS avg_rating,
    round(AVG(total_duration_min), 2) AS avg_watch_duration, 
    countif(is_rewatch = true) AS rewatch_count
FROM watches
group by country
order by watch_count desc
