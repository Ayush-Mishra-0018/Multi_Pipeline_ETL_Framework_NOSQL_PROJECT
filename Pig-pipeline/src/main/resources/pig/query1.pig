-- For each day and HTTP status code, how many requests and bytes sent
raw = LOAD '$INPUT_DIR' USING PigStorage('\t') AS (host:chararray, rawTimestamp:chararray, formattedDate:chararray, hour:int, method:chararray, path:chararray, protocol:chararray, status:int, bytes:long, batchId:int);

grp = GROUP raw BY (formattedDate, status);
res = FOREACH grp GENERATE group.formattedDate AS log_date, group.status AS status_code, COUNT(raw) AS request_count, SUM(raw.bytes) AS total_bytes, '$BATCH_ID' AS batchId;

STORE res INTO '$OUTPUT_DIR' USING PigStorage('\t');
