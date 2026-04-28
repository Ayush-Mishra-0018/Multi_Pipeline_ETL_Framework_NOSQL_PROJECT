# Multi Pipeline ETL Framework for Web Server Log Analytics

**Team Members**
- Ayush Mishra (IMT2023129)
- Harsh Sinha (IMT2023571)
- Syed Naveed Mohammed (IMT2023119)
- Santhosh Vodnala (IMT2023622)

**GitHub Repository:** [Multi Pipeline ETL Framework Project](https://github.com/Ayush-Mishra-0018/Multi_Pipeline_ETL_Framework_NOSQL_PROJECT)

---

## Table of Contents

1. [Introduction](#1-introduction)
2. [Proposed Architecture and Workflow](#2-proposed-architecture-and-workflow)
3. [Parsing Strategy and Batching Approach](#3-parsing-strategy-and-batching-approach)
4. [Relational Reporting Database Schema](#4-relational-reporting-database-schema)
5. [Query Outputs and Demonstrations](#5-query-outputs-and-demonstrations)
6. [Runtime Module](#6-runtime-module)
7. [Conclusion](#conclusion)

---

## Project Structure

```
./
├── README.md
├── common
│   ├── pom.xml
│   └── src/main/java/com/example
│       ├── config
│       │   ├── AppProperties.java
│       │   └── ConfigReader.java
│       ├── model
│       │   ├── BatchResult.java
│       │   └── ParsedLog.java
│       └── util
│           ├── BatchProcessor.java
│           ├── BatchReader.java
│           └── LogParser.java
├── data
│   ├── NASA_access_log_Aug95
│   └── NASA_access_log_Jul95
├── mongo-pipeline
│   ├── pom.xml
│   └── src/main/java/com/example/mongo
│       ├── queries
│       │   ├── Query1_DailyTraffic_Global.java
│       │   ├── Query2_TopResources.java
│       │   └── Query3_HourlyErrorAnalysis.java
│       ├── runner
│       │   └── QueryRunner.java
│       └── service
│           ├── MongoConnection.java
│           └── MongoInsertService.java
├── postgres-loader
│   ├── pom.xml
│   └── src/main/java/com/example/postgres/service
│       ├── PostgresInsertService.java
│       ├── PostgresReaderService.java
│       └── PostgresSchemaInitializer.java
├── reporting
│   ├── pom.xml
│   └── src/main/java/com/example/reporting
│       ├── MongoPipelineMain.java
│       └── RunModule.java
└── pom.xml
```

### Module Responsibilities

| Module | Package | Responsibility |
|---|---|---|
| `common` | `config`, `model`, `util` | Shared parsing, batching, and data model classes |
| `mongo-pipeline` | `queries`, `runner`, `service` | MongoDB aggregation queries and insert service |
| `postgres-loader` | `postgres/service` | PostgreSQL schema init, insert, and read services |
| `reporting` | `reporting` | Entry point — orchestrates the full pipeline run |
| `data` | — | Raw NASA HTTP server log files |

---

## 1. Introduction

This report presents the implementation status and architecture of the MongoDB-based ETL pipeline for web server log analysis. It highlights the current progress, system design, and workflow used to process and aggregate log data using MongoDB, along with storage of results in PostgreSQL.

---

## 2. Proposed Architecture and Workflow

### System Overview

The system implements a MongoDB-based ETL pipeline for processing large-scale web server logs. It focuses on transforming raw log data into structured format, processing it in batches, and performing efficient aggregation using MongoDB.

The architecture is modular, separating parsing, batching, storage, and query execution. This design enables scalability and simplifies extension to other pipelines in future phases.

### End-to-End Data Flow

The data flows through the following stages:

1. **Input Layer** — Raw NASA log files are read from disk.
2. **Parsing Layer** — Logs are converted into structured records containing fields such as date, status code, and bytes.
3. **Batching Layer** — Records are grouped into batches to enable scalable and parallel processing.
4. **MongoDB Storage** — Each batch is stored in a separate MongoDB collection.
5. **Processing Layer** — Aggregation queries are executed on each batch collection using MongoDB pipelines.
6. **Global Aggregation** — Partial results from all batches are merged to produce final aggregated results.
7. **Output Layer** — Final results are enriched with metadata and stored in PostgreSQL for reporting.

### Pipeline Backend (MongoDB)

The MongoDB pipeline serves as the primary execution backend in this phase. It uses aggregation pipelines to compute query results efficiently.

Batch-level processing is executed in parallel using multi-threading, allowing multiple collections to be processed simultaneously. Aggregation results are then combined to produce a global output, ensuring correctness across all batches.

### Design Principles

| Principle | Description |
|---|---|
| **Modularity** | Parsing, batching, and query execution are independent components |
| **Scalability** | Batch-based processing and parallel execution handle large datasets |
| **Extensibility** | Architecture can be extended to support additional pipelines |
| **Consistency** | Aggregation logic ensures accurate results across all batches |

---

## 3. Parsing Strategy and Batching Approach

This section describes how raw NASA HTTP server log files are read, parsed into structured records, grouped into batches for processing, and how malformed entries are handled throughout the pipeline.

The implementation is spread across four classes in the `common` module: `LogParser`, `BatchProcessor`, `BatchReader`, and `BatchResult`.

### 3.1 Log Format and Structure

The input dataset consists of two raw NASA HTTP server access log files:
- `NASA_access_log_Jul95`
- `NASA_access_log_Aug95`

Together they total approximately **373 MB** of uncompressed log data.

Each line follows the **Combined Log Format (CLF)**, a standard format used by Apache and NCSA HTTP servers.

**Sample log entry:**
```
199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245
```

**Log fields:**

| Field | Example Value | Description |
|---|---|---|
| Host | `199.72.81.55` | Client hostname or IP address |
| Ident | `-` | RFC 1413 identity (always `-`) |
| Auth User | `-` | Authenticated username (always `-`) |
| Timestamp | `01/Jul/1995:00:00:01 -0400` | Request date and time with timezone |
| Request | `GET /history/apollo/ HTTP/1.0` | Full HTTP request line |
| Status Code | `200` | HTTP response status code |
| Bytes | `6245` | Response size in bytes (`-` if unknown) |

### 3.2 Parsing Strategy

Parsing is handled entirely by the `LogParser` class (`common/src/main/java/com/example/util/LogParser.java`). The parser applies a single compiled regular expression to each raw log line and extracts all relevant fields in one pass.

#### Regular Expression

```
^(\S+) \S+ \S+ \[(.*?)\] "(.*?)" (\d{3}) (\S+)$
```

**Capture groups:**

| Group | Pattern | Field | Description |
|---|---|---|---|
| 1 | `(\S+)` | Host | Non-whitespace token — client IP or hostname |
| 2 | `(.*?)` | Raw Timestamp | Content inside `[ ]` — full date-time string |
| 3 | `(.*?)` | Request | Content inside `" "` — HTTP method, path, protocol |
| 4 | `(\d{3})` | Status Code | Exactly 3 digits — HTTP response code |
| 5 | `(\S+)` | Bytes | Non-whitespace — response size or `-` |

#### Field Extraction and Transformation

- **Timestamp parsing:** The raw timestamp is split on `:` to separate the date from the hour. The date is re-formatted to ISO-8601 (`yyyy-MM-dd`). The hour is extracted as an integer.
- **Request line parsing:** The request string is split on whitespace into method, path, and protocol. Fewer than three tokens → record flagged as malformed.
- **Bytes field:** `"-"` is stored as `0`; otherwise parsed as `long`.
- **Batch ID injection:** Each `ParsedLog` record is stamped with its `batchId` for downstream filtering and aggregation.

**`ParsedLog` model fields:**
```java
private String  host;
private String  rawTimestamp;
private String  date;        // ISO-8601 format: yyyy-MM-dd
private int     hour;        // 0-23
private String  method;      // GET, POST, HEAD, etc.
private String  path;        // Requested resource path
private String  protocol;    // HTTP/1.0 or HTTP/1.1
private int     status;      // HTTP status code
private long    bytes;       // Response size (0 if unknown)
private boolean malformed;   // true if parsing failed
private int     batchId;     // Batch this record belongs to
```

### 3.3 Batching Approach

Large log files are not loaded entirely into memory. They are streamed and consumed in fixed-size batches, keeping memory usage bounded and enabling parallel query execution.

#### BatchReader — Streaming Lines from Disk

`BatchReader` wraps a `BufferedReader` opened with `ISO-8859-1` encoding (required for non-UTF-8 byte sequences in the NASA logs). It implements `AutoCloseable` for safe use in try-with-resources.

```java
public List<String> readNextBatch(int batchSize) throws IOException {
    List<String> lines = new ArrayList<>(batchSize);
    String line;
    while (lines.size() < batchSize &&
           (line = reader.readLine()) != null) {
        lines.add(line);
    }
    return lines;
}
```

#### BatchProcessor — Parsing a Batch

```java
public static BatchResult processBatch(
        List<String> rawLines, int batchId) {

    List<ParsedLog> parsedLogs = new ArrayList<>(rawLines.size());
    int malformedCount = 0;

    for (String line : rawLines) {
        ParsedLog log = LogParser.parse(line, batchId);
        if (log.isMalformed()) malformedCount++;
        parsedLogs.add(log);
    }

    return BatchResult.builder()
            .batchId(batchId)
            .totalRecords(rawLines.size())
            .malformedRecords(malformedCount)
            .validRecords(rawLines.size() - malformedCount)
            .parsedLogs(parsedLogs)
            .build();
}
```

#### Batch Size Configuration

```properties
batch.size=25000
input.file.paths=./data/NASA_access_log_Jul95,./data/NASA_access_log_Aug95
mongo.clear.before.run=true
```

With a batch size of 25,000 and ~3.46 million total records, the pipeline produces **139 batches** per full run. Each batch gets a monotonically incrementing `batchId` starting from 1.

#### Multi-File Processing

The pipeline iterates over all input file paths. For each file a fresh `BatchReader` is opened and batches are consumed until EOF. The `batchId` counter is **not** reset between files, ensuring globally unique identifiers across the entire run.

### 3.4 Handling Malformed Records

A record is classified as malformed if:

- The raw line does not match the regex pattern (missing fields, unexpected delimiters, truncated lines).
- The request field contains fewer than three whitespace-separated tokens.
- Any numeric field (`status`, `bytes`, `hour`) throws a `NumberFormatException`.
- Any other unexpected exception is raised inside `parse()`.

Malformed records are **not discarded**. They are stored in `parsed_logs` with `malformed = true`. Only valid records are written to `filtered_logs` and the per-batch `filtered_logs_batch_N` collections used for aggregation.

### 3.5 Pipeline Summary Output

| Metric | Value |
|---|---|
| Total Records Processed | 3,461,613 |
| Total Valid Records | 3,461,612 |
| Total Malformed Records | 1 |
| Total Batches | 139 |
| Average Batch Size | 24,903.69 |
| Total Execution Time (ms) | 40,487 |

---

## 4. Relational Reporting Database Schema

### Query 1 Reporting Table — `public.query_1`

Stores daily traffic summaries aggregated by log date and status code.

| Column | Type | Purpose |
|---|---|---|
| id | SERIAL | Primary key |
| log_date | DATE | Log date of requests |
| status_code | INTEGER | HTTP response status code |
| request_count | BIGINT | Total number of requests |
| total_bytes | BIGINT | Total bytes transferred |
| batch_id | TEXT | Contributing batch IDs |
| run_id | TEXT | Unique execution identifier |
| pipeline_name | TEXT | Backend used for execution |
| executed_at | TIMESTAMP | Execution timestamp |

### Query 2 Reporting Table — `public.query_2`

Stores top requested resource analytics with execution metadata.

| Column | Type | Purpose |
|---|---|---|
| id | SERIAL | Primary key |
| resource_path | TEXT | Requested resource path |
| request_count | INTEGER | Total number of requests |
| total_bytes | BIGINT | Total bytes transferred |
| distinct_hosts | INTEGER | Number of unique requesting hosts |
| batch_id | TEXT | Contributing batch IDs |
| run_id | TEXT | Unique execution identifier |
| pipeline_name | TEXT | Backend used for execution |
| executed_at | TIMESTAMP | Execution timestamp |

### Query 3 Reporting Table — `public.query_3`

Stores hourly error analysis including error counts, total requests, and computed error rates.

| Column | Type | Purpose |
|---|---|---|
| log_date | TEXT | Log date of requests |
| log_hour | INTEGER | Hour of the request |
| error_request_count | INTEGER | Number of error requests (4xx/5xx) |
| total_request_count | INTEGER | Total number of requests |
| error_rate | DOUBLE PRECISION | Percentage of error requests |
| distinct_error_hosts | INTEGER | Unique hosts causing errors |
| batch_id | TEXT | Contributing batch IDs |
| run_id | TEXT | Unique execution identifier |
| pipeline_name | TEXT | Backend used for execution |
| executed_at | TIMESTAMP | Execution timestamp |

### Metadata Columns (Common Across All Queries)

| Column | Description |
|---|---|
| `pipeline_name` | Identifies the execution backend (e.g., MongoDB) |
| `run_id` | UUID generated once per query execution — groups all rows from the same run |
| `batch_id` | Set of contributing batches, e.g. `1+2+3+4` |
| `executed_at` | Timestamp of PostgreSQL insertion — useful for historical tracking |

---

## 5. Query Outputs and Demonstrations

Results are directly inserted into PostgreSQL using a shared insertion service. All results are stored in a database named `mongodb` for pipeline identification.

### Query 1: Daily Traffic Summary

Aggregates logs by **log date** and **status code**, computing total requests and total bytes transferred.

**Implementation:**
- Each batch is stored in a separate MongoDB collection.
- Aggregation via MongoDB's `$group` operator per batch.
- Partial results merged across all batches.
- Multi-threaded batch processing for performance.
- Results sorted by log date and status code.

**Observations:**
- Status code `200` dominates traffic, indicating mostly successful requests.
- Status codes `304` and `404` show zero byte transfers, as expected for cached or failed responses.
- Batch identifiers confirm multiple batches contribute to the final result.

### Query 2: Top Requested Resources

Computes the **top 20 requested resource paths**, with total requests, total bytes, and distinct host counts.

**Implementation:**
- Per-batch aggregation grouped by `path` using `$group`.
- Partial results merged in Java to compute global aggregates.
- Global top 20 selected by request count ranking.
- Output displayed in ascending order of request count.

**Observations:**
- Static resources (images, icons, logos) dominate due to repeated browser requests.
- NASA logo and homepage assets appear among the most frequently requested entries.
- High distinct host counts confirm popularity across many unique clients.

### Query 3: Hourly Error Analysis

Aggregates logs by **log date** and **log hour**, computing error counts (HTTP 400–599), total requests, error rate percentage, and distinct error hosts.

**Implementation:**
- Per-batch aggregation grouped by `log_date` and `log_hour`.
- Error requests identified by status codes 400–599.
- Distinct error hosts collected using `$addToSet`.
- Partial results merged across all batches.
- Results sorted by log date and hour.

**Observations:**
- Hours with higher error counts indicate peak failure periods.
- Error rate highlights time intervals with disproportionate failure traffic.
- Distinct error host counts reveal whether failures are widespread or isolated.

---

## 6. Runtime Module

The runtime module (`MongoPipelineMain`) orchestrates the full pipeline end-to-end:

1. Reads configuration from `app.properties`.
2. Optionally clears existing MongoDB collections.
3. Iterates over input files, streaming batches via `BatchReader`.
4. Processes each batch through `BatchProcessor` and inserts into MongoDB.
5. Executes all three queries in parallel using a thread pool.
6. Inserts enriched results into PostgreSQL.
7. Prints and persists the final pipeline summary.

---

## Conclusion

The MongoDB pipeline successfully demonstrates:

- **Efficient log parsing** using a single-pass regex with field-level transformations.
- **Scalable batching** with bounded memory usage and globally unique batch IDs across files.
- **Parallel query execution** via multi-threaded aggregation over per-batch MongoDB collections.
- **Correct global aggregation** by merging partial results from all batches.
- **Persistent reporting** through direct PostgreSQL insertion with full execution metadata.

The system processed **3,461,613 records** across **139 batches** in under **41 seconds**, with only **1 malformed record** across the entire NASA dataset.
