-- =============================================================
-- QUERY 3: Hourly Error Analysis
--
-- Groups nasa_filtered_logs by (log_date_raw, log_hour) globally.
-- Equivalent to Mongo Query3_HourlyErrorAnalysis which groups by
-- ($date, $hour) and uses $cond to count errors (status 400-599),
-- $addToSet + $$REMOVE for distinct error hosts.
--
-- nasa_filtered_logs column references:
--   log_date_raw  STRING  — e.g. '01/Jul/1995'
--   log_hour      INT     — 0..23
--   status        INT     — HTTP status code
--   host          STRING  — client hostname / IP
--   batch_id      INT     — 1 or 2
--
-- error_rate formula: ROUND(errors * 100.0 / total, 2)
-- Matches Mongo: Math.round((errors * 10000.0 / total)) / 100.0
--
-- COUNT(DISTINCT CASE WHEN status BETWEEN 400 AND 599 THEN host END)
-- counts only hosts of error requests; NULL hosts (non-error rows)
-- are ignored by COUNT DISTINCT — matches Mongo's $$REMOVE semantics.
--
-- Output TSV (7 columns) read by HiveQueryRunner.runQuery3():
--   col 0: log_date             (String)
--   col 1: log_hour             (INT)
--   col 2: error_request_count  (INT)
--   col 3: total_request_count  (INT)
--   col 4: error_rate           (DOUBLE)
--   col 5: distinct_error_hosts (INT)
--   col 6: batch_id             (String)
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
