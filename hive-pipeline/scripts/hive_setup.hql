-- =============================================================
-- HIVE SETUP SCRIPT: NASA Log ETL
-- Hive 3.1.3 compatible
-- NO CTAS syntax used anywhere
-- =============================================================


-- =============================================================
-- CLEANUP  (drop in reverse dependency order)
-- =============================================================

DROP TABLE IF EXISTS nasa_filtered_logs;
DROP TABLE IF EXISTS nasa_malformed_logs;
DROP TABLE IF EXISTS nasa_parsed_logs;
DROP TABLE IF EXISTS nasa_raw_logs;
DROP TABLE IF EXISTS nasa_raw_stage_b1;
DROP TABLE IF EXISTS nasa_raw_stage;


-- =============================================================
-- RAW LOG TABLE
-- =============================================================

CREATE TABLE nasa_raw_logs (
                               raw_line STRING,
                               batch_id INT
) STORED AS PARQUET;


-- =============================================================
-- STAGING TABLE  (shared landing zone for LOAD DATA)
-- =============================================================

CREATE TABLE nasa_raw_stage (
                                batch_id INT,
                                raw_line STRING
)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\000'
    STORED AS TEXTFILE;

-- =============================================================
-- LOAD PRE-PROCESSED DATA  →  stage
-- =============================================================

LOAD DATA INPATH '${hiveconf:hdfsBase}/nasa_raw/staged'
    OVERWRITE INTO TABLE nasa_raw_stage;

-- =============================================================
-- INSERT OVERWRITE INTO RAW LOGS
-- =============================================================

INSERT OVERWRITE TABLE nasa_raw_logs
SELECT raw_line, batch_id FROM nasa_raw_stage;

-- =============================================================
-- DROP STAGING TABLES
-- =============================================================

DROP TABLE IF EXISTS nasa_raw_stage;

-- =============================================================
-- PARSED LOG TABLE
-- =============================================================

CREATE TABLE IF NOT EXISTS nasa_parsed_logs (
                                                batch_id      INT,
                                                raw_line      STRING,
                                                host          STRING,
                                                raw_timestamp STRING,
                                                request       STRING,
                                                status_str    STRING,
                                                bytes_str     STRING
) STORED AS PARQUET;

-- Using INSERT OVERWRITE wipes old duplicate rows
INSERT OVERWRITE TABLE nasa_parsed_logs
SELECT
    batch_id,
    raw_line,
    regexp_extract(RTRIM(raw_line), '^(\\S+) \\S+ \\S+ \\[(.*?)\\] "(.*?)" (\\d{3}) (\\S+)$', 1) AS host,
    regexp_extract(RTRIM(raw_line), '^(\\S+) \\S+ \\S+ \\[(.*?)\\] "(.*?)" (\\d{3}) (\\S+)$', 2) AS raw_timestamp,
    regexp_extract(RTRIM(raw_line), '^(\\S+) \\S+ \\S+ \\[(.*?)\\] "(.*?)" (\\d{3}) (\\S+)$', 3) AS request,
    regexp_extract(RTRIM(raw_line), '^(\\S+) \\S+ \\S+ \\[(.*?)\\] "(.*?)" (\\d{3}) (\\S+)$', 4) AS status_str,
    regexp_extract(RTRIM(raw_line), '^(\\S+) \\S+ \\S+ \\[(.*?)\\] "(.*?)" (\\d{3}) (\\S+)$', 5) AS bytes_str
FROM nasa_raw_logs;


-- =============================================================
-- MALFORMED LOG TABLE
-- =============================================================

CREATE TABLE IF NOT EXISTS nasa_malformed_logs (
                                                   batch_id INT,
                                                   record   STRING
) STORED AS PARQUET;

INSERT OVERWRITE TABLE nasa_malformed_logs
SELECT
    batch_id,
    raw_line
FROM nasa_parsed_logs
WHERE
   -- EXACT MATCH TO PIG
    host = '' OR host IS NULL;


-- =============================================================
-- FILTERED / VALID LOG TABLE
-- =============================================================

CREATE TABLE IF NOT EXISTS nasa_filtered_logs (
                                                  batch_id      INT,
                                                  host          STRING,
                                                  raw_timestamp STRING,
                                                  log_date_raw  STRING,
                                                  log_hour      INT,
                                                  method        STRING,
                                                  path          STRING,
                                                  protocol      STRING,
                                                  status        INT,
                                                  bytes         BIGINT
) STORED AS PARQUET;

INSERT OVERWRITE TABLE nasa_filtered_logs
SELECT
    batch_id,
    host,
    raw_timestamp,
    split(raw_timestamp, ':')[0]              AS log_date_raw,
    CAST(split(raw_timestamp, ':')[1] AS INT) AS log_hour,

    -- EXACT MATCH TO PIG: Insert empty strings '' if parts are missing
    CASE WHEN size(split(TRIM(request), ' ')) >= 3 THEN split(TRIM(request), ' ')[0] ELSE '' END AS method,
    CASE WHEN size(split(TRIM(request), ' ')) >= 3 THEN split(TRIM(request), ' ')[1] ELSE '' END AS path,
    CASE WHEN size(split(TRIM(request), ' ')) >= 3 THEN split(TRIM(request), ' ')[2] ELSE '' END AS protocol,

    CAST(status_str AS INT)                   AS status,
    CASE
        WHEN bytes_str = '-' THEN CAST(0 AS BIGINT)
        ELSE CAST(bytes_str AS BIGINT)
        END                                       AS bytes
FROM nasa_parsed_logs
WHERE
  -- EXACT MATCH TO PIG
    host != '' AND host IS NOT NULL;