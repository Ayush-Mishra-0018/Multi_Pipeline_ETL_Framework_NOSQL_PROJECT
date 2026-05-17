-- =============================================================
-- QUERY 2: Top 20 Requested Resources
-- =============================================================

SELECT
    resource_path,
    request_count,
    total_bytes,
    distinct_hosts,
    batch_id
FROM (
         SELECT
             path                                                                AS resource_path,
             COUNT(*)                                                            AS request_count,
             SUM(bytes)                                                          AS total_bytes,
             COUNT(DISTINCT host)                                                AS distinct_hosts,
             CONCAT_WS('+', SORT_ARRAY(COLLECT_SET(CAST(batch_id AS STRING))))   AS batch_id
         FROM nasa_filtered_logs
         GROUP BY path
         ORDER BY request_count DESC, resource_path ASC
         LIMIT 20
     ) top20
ORDER BY
    request_count ASC,
    resource_path ASC;