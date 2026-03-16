SELECT
    fw.content_id, 
    dc.content_title,
    AVG(fw.rating) AS avg_rating
FROM `netflix-pipeline-490116.netflix_dataset.fact_watches` fw
JOIN `netflix-pipeline-490116.netflix_dataset.dim_content` dc ON fw.content_id = dc.content_id
WHERE fw.rating IS NOT NULL AND fw.timestamp >= '2026-01-01'
GROUP BY fw.content_id, dc.content_title
HAVING AVG(fw.rating) > 3
ORDER BY avg_rating DESC;