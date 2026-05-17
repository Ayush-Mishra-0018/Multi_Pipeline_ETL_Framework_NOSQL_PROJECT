-- =============================================================
-- QUERY 1: Daily Traffic Summary
--
-- Groups nasa_filtered_logs by (log_date_raw, status) globally
-- across all batches.  Equivalent to Mongo Query1_DailyTraffic_Global
-- which groups by ($date, $status) and sums request_count + total_bytes.
--
-- nasa_filtered_logs column references:
--   log_date_raw STRING  — e.g. '01/Jul/1995'
--   status       INT     — HTTP status code
--   bytes        BIGINT  — bytes transferred (0 when bytes_str was '-')
--   batch_id     INT     — 1 or 2
--
-- Output TSV (5 columns) read by HiveQueryRunner.runQuery1():
--   col 0: log_date   (String  -> parseDateToSql -> java.sql.Date)
--   col 1: status_code (INT)
--   col 2: request_count (LONG)
--   col 3: total_bytes   (LONG)
--   col 4: batch_id      (String, e.g. '1', '2', '1+2')
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
