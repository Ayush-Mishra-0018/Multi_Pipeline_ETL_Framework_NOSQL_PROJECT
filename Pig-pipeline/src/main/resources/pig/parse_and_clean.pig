-- parse_and_clean.pig

raw_data = LOAD '$INPUT_FILE' USING TextLoader() AS (line:chararray);

-- Parse the line while preserving original input
parsed = FOREACH raw_data GENERATE
    line,
    REGEX_EXTRACT_ALL(
        line,
        '^(\\S+) \\S+ \\S+ \\[(.*?)\\] "(.*?)" (\\d{3}) (\\S+)$'
    ) AS fields;

-- Split valid and malformed records
SPLIT parsed INTO
    valid_logs IF fields IS NOT NULL,
    malformed_logs IF fields IS NULL;

-- Extract parsed fields from valid logs
extracted_valid = FOREACH valid_logs GENERATE
    line,
    (chararray)fields.$0 AS host,
    (chararray)fields.$1 AS rawTimestamp,
    (chararray)fields.$2 AS request,
    (int)fields.$3 AS status,
    (chararray)fields.$4 AS bytes;

-- Split timestamp and request parts
split_valid = FOREACH extracted_valid GENERATE
    host,
    rawTimestamp,
    STRSPLIT(rawTimestamp, ':') AS date_parts,
    STRSPLIT(TRIM(REPLACE(request, '\\s+', ' ')), ' ') AS req_parts,
    status,
    bytes,
    (int)'$BATCH_ID' AS batchId;

-- Format final cleaned output
formatted_valid = FOREACH split_valid GENERATE
    host,
    rawTimestamp,
    ToString(
        ToDate((chararray)date_parts.$0, 'dd/MMM/yyyy'),
        'yyyy-MM-dd'
    ) AS formattedDate,

    (int)(date_parts.$1) AS hour,

    (SIZE(req_parts) > 0 ? (chararray)req_parts.$0 : '') AS method,

    (SIZE(req_parts) > 1 ? (chararray)req_parts.$1 : '') AS path,

    (SIZE(req_parts) > 2 ? (chararray)req_parts.$2 : '') AS protocol,

    status,

    (bytes == '-' ? 0L : (long)bytes) AS bytes_val,

    batchId;

-- Keep malformed raw lines separately
just_malformed = FOREACH malformed_logs GENERATE line;

-- Store outputs
STORE formatted_valid INTO '$VALID_OUTPUT' USING PigStorage('\t');

STORE just_malformed INTO '$MALFORMED_OUTPUT' USING PigStorage();