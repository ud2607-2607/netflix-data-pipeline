{{ config(materialized='view') }}

with watches as (
    SELECT * from {{ ref('fct_watches') }}
)

SELECT 
    COUNT(*) AS watch_count,
    content_title, 
    content_type,
    genre
FROM watches
GROUP BY content_title, content_type, genre
ORDER BY watch_count DESC
LIMIT 10