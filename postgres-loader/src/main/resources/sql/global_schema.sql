CREATE TABLE IF NOT EXISTS run_metadata
(

    run_id
    INTEGER
    GENERATED
    ALWAYS AS
    IDENTITY
    PRIMARY
    KEY,

    pipeline_name
    VARCHAR
(
    255
) NOT NULL,

    query_name INTEGER NOT NULL
    CHECK
(
    query_name
    >=
    1
    AND
    query_name
    <=
    4
),

    runtime NUMERIC
(
    10,
    3
) NOT NULL,

    execution_timestamp TIMESTAMP WITH TIME ZONE
    NOT NULL DEFAULT CURRENT_TIMESTAMP
    );

CREATE TABLE IF NOT EXISTS batch_metadata
(

    run_id
    INTEGER
    PRIMARY
    KEY
    REFERENCES
    run_metadata
(
    run_id
),

    pipeline_name VARCHAR
(
    255
) NOT NULL,

    total_records INTEGER NOT NULL,

    total_valid INTEGER NOT NULL,

    total_malformed INTEGER NOT NULL DEFAULT 0,

    total_batches INTEGER NOT NULL,

    avg_batch_size DOUBLE PRECISION NOT NULL,

    execution_time_ms INTEGER NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE
    NOT NULL DEFAULT CURRENT_TIMESTAMP
    );