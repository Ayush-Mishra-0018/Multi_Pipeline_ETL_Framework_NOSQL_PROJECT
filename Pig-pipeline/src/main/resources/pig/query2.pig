-- Top 20 most requested resources with total request counts, total bytes sent, and unique hosts that made those requests
raw = LOAD '$INPUT_DIR' USING PigStorage('\t') AS (host:chararray, rawTimestamp:chararray, formattedDate:chararray, hour:int, method:chararray, path:chararray, protocol:chararray, status:int, bytes:long, batchId:int);

valid_raw = FILTER raw BY path IS NOT NULL AND path != '' AND protocol IS NOT NULL AND protocol != '';

grp = GROUP valid_raw BY path;
res = FOREACH grp {
    unique_hosts = DISTINCT valid_raw.host;
    GENERATE group AS resource_path, COUNT(valid_raw) AS request_count, SUM(valid_raw.bytes) AS total_bytes, unique_hosts AS hosts, '$BATCH_ID' AS batchId;
}

STORE res INTO '$OUTPUT_DIR' USING PigStorage('\t');
