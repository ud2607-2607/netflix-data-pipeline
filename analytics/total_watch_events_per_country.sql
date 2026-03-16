SELECT 
       u.country,
       COUNT(fw.watch_id) as total_count
FROM `netflix-pipeline-490116.netflix_dataset.fact_watches` fw
JOIN `netflix-pipeline-490116.netflix_dataset.dim_users` u ON fw.user_id = u.user_id 
WHERE fw.timestamp >= '2026-01-01'
GROUP BY country
ORDER BY COUNT(fw.watch_id) DESC