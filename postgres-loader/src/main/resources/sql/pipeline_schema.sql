CREATE TABLE IF NOT EXISTS query_1
(

    id
    SERIAL
    PRIMARY
    KEY,

    log_date
    DATE,

    status_code
    INT,

    request_count
    BIGINT,

    total_bytes
    BIGINT,

    batch_id
    TEXT
);

CREATE TABLE IF NOT EXISTS query_2
(

    id
    SERIAL
    PRIMARY
    KEY,

    resource_path
    TEXT,

    request_count
    INT,

    total_bytes
    BIGINT,

    distinct_hosts
    INT,

    batch_id
    TEXT
);

CREATE TABLE IF NOT EXISTS query_3
(

    id
    SERIAL
    PRIMARY
    KEY,

    log_date
    TEXT,

    log_hour
    INT,

    error_request_count
    INT,

    total_request_count
    INT,

    error_rate
    DOUBLE
    PRECISION,

    distinct_error_hosts
    INT,

    batch_id
    TEXT
);

CREATE TABLE IF NOT EXISTS malformed_record_summary
(

    id
    INTEGER
    GENERATED
    ALWAYS AS
    IDENTITY
    PRIMARY
    KEY,

    batch_id
    INTEGER
    NOT
    NULL,

    record
    VARCHAR
(
    4096
)
    );