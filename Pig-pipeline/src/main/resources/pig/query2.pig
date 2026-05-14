raw = LOAD '$INPUT_DIR' USING PigStorage('\t') AS (host:chararray, rawTimestamp:chararray, formattedDate:chararray, hour:int, method:chararray, path:chararray, protocol:chararray, status:int, bytes:long, batchId:int);

grp = GROUP raw BY path;
res = FOREACH grp {
    unique_hosts = DISTINCT raw.host;
    GENERATE group AS resource_path, COUNT(raw) AS request_count, SUM(raw.bytes) AS total_bytes, unique_hosts AS hosts, '$BATCH_ID' AS batchId;
}

STORE res INTO '$OUTPUT_DIR' USING PigStorage('\t');
