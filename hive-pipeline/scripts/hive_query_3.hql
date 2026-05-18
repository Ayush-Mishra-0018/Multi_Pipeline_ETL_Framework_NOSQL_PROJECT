-- =============================================================
-- QUERY 3: Hourly Error Analysis
-- =============================================================

SELECT
  log_date_raw                                                                    AS log_date,
  log_hour,
  SUM(CASE WHEN status >= 400 AND status <= 599 THEN 1 ELSE 0 END)               AS error_request_count,
  COUNT(*)                                                                         AS total_request_count,
  ROUND(
    SUM(CASE WHEN status >= 400 AND status <= 599 THEN 1 ELSE 0 END) * 10000.0
    / COUNT(*)
  ) / 100.0                                                                        AS error_rate,
  COUNT(DISTINCT CASE WHEN status >= 400 AND status <= 599 THEN host END)         AS distinct_error_hosts,
  CONCAT_WS('+', SORT_ARRAY(COLLECT_SET(CAST(batch_id AS STRING))))               AS batch_id
FROM nasa_filtered_logs
GROUP BY
  log_date_raw,
  log_hour
ORDER BY
  from_unixtime(unix_timestamp(log_date_raw, 'dd/MMM/yyyy')) ASC,
  log_hour ASC;
