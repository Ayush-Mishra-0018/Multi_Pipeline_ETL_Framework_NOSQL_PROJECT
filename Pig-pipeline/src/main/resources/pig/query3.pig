-- For each date and hour, number of requests, numbers of errors, total requests, and the error hosts
raw = LOAD '$INPUT_DIR' USING PigStorage('\t') AS (host:chararray, rawTimestamp:chararray, formattedDate:chararray, hour:int, method:chararray, path:chararray, protocol:chararray, status:int, bytes:long, batchId:int);

grp = GROUP raw BY (formattedDate, hour);
res = FOREACH grp {
    errors = FILTER raw BY status >= 400 AND status <= 599;
    unique_error_hosts = DISTINCT errors.host;
    GENERATE group.formattedDate AS log_date, group.hour AS log_hour, COUNT(errors) AS error_request_count, COUNT(raw) AS total_request_count, unique_error_hosts AS hosts, '$BATCH_ID' AS batchId;
}

STORE res INTO '$OUTPUT_DIR' USING PigStorage('\t');
