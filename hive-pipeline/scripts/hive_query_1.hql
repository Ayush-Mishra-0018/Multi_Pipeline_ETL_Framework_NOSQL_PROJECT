-- =============================================================
-- QUERY 1: Daily Traffic Summary
-- =============================================================

SELECT
  log_date_raw                                                        AS log_date,
  status                                                              AS status_code,
  COUNT(*)                                                            AS request_count,
  SUM(bytes)                                                          AS total_bytes,
  CONCAT_WS('+', SORT_ARRAY(COLLECT_SET(CAST(batch_id AS STRING))))  AS batch_id
FROM nasa_filtered_logs
GROUP BY
  log_date_raw,
  status
ORDER BY
  from_unixtime(unix_timestamp(log_date_raw, 'dd/MMM/yyyy')) ASC,
  status ASC;
