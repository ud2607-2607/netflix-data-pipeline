SELECT 
    c.content_title, 
    COUNT(*) AS watch_count
FROM
    `netflix-pipeline-490116.netflix_dataset.fact_watches` w
JOIN
    `netflix-pipeline-490116.netflix_dataset.dim_content` c
ON
    w.content_id = c.content_id
WHERE w.timestamp >= '2026-01-01' 
GROUP BY c.content_title
ORDER BY watch_count DESC
LIMIT 10;