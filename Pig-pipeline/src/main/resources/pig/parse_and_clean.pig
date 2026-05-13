-- parse_and_clean.pig

raw_data = LOAD '$INPUT_FILE' USING TextLoader() AS (line:chararray);

-- Parse the line using Regex
parsed = FOREACH raw_data GENERATE 
    FLATTEN(REGEX_EXTRACT_ALL(line, '^(\\S+) \\S+ \\S+ \\[(.*?)\\] "(.*?)" (\\d{3}) (\\S+)$')) 
    AS (host:chararray, rawTimestamp:chararray, request:chararray, status:int, bytes:chararray), line;

-- Split valid and malformed records
SPLIT parsed INTO 
    valid_logs IF host IS NOT NULL, 
    malformed_logs IF host IS NULL;

-- Step 1: Perform the splits in a FOREACH
split_valid = FOREACH valid_logs GENERATE
    host,
    rawTimestamp,
    STRSPLIT(rawTimestamp, ':') AS date_parts,
    STRSPLIT(request, ' ') AS req_parts,
    status,
    bytes,
    (int)'$BATCH_ID' AS batchId;

-- Step 2: Format the final output
formatted_valid = FOREACH split_valid GENERATE
    host,
    rawTimestamp,
    ToString(ToDate((chararray)date_parts.$0, 'dd/MMM/yyyy'), 'yyyy-MM-dd') AS formattedDate,
    (int)(date_parts.$1) AS hour,
    (SIZE(req_parts) > 0 ? (chararray)req_parts.$0 : '') AS method,
    (SIZE(req_parts) > 1 ? (chararray)req_parts.$1 : '') AS path,
    (SIZE(req_parts) > 2 ? (chararray)req_parts.$2 : '') AS protocol,
    status,
    (bytes == '-' ? 0L : (long)bytes) AS bytes_val,
    batchId;

just_malformed = FOREACH malformed_logs GENERATE line;

STORE formatted_valid INTO '$VALID_OUTPUT' USING PigStorage('\t');
STORE just_malformed INTO '$MALFORMED_OUTPUT' USING PigStorage();
