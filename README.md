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
6. [Pipeline Performance Comparison](#6-pipeline-performance-comparison)
7. [Runtime Module](#7-runtime-module)
8. [Conclusion](#8-conclusion)

---

## Project Structure

```
./
├── README.md
├── common/
├── data/
├── hive-pipeline/
├── map-reduce/
├── mongo-pipeline/
├── Pig-pipeline/
├── postgres-loader/
├── reporting/
└── pom.xml
```

### Module Responsibilities

| Module | Responsibility |
|---|---|
| `common` | Shared parsing, batching, and data model classes |
| `mongo-pipeline` | MongoDB aggregation queries and insert service |
| `hive-pipeline` | Apache Hive execution backend and ETL orchestrator |
| `map-reduce` | Native Hadoop MapReduce backend |
| `Pig-pipeline` | Apache Pig scripting backend |
| `postgres-loader` | PostgreSQL schema init, insert, and read services |
| `reporting` | Central orchestration module to run individual pipelines or all of them |
| `data` | Raw NASA HTTP server log files |

---

## 1. Introduction

This report presents the implementation and architecture of a Multi-Pipeline NoSQL ETL framework for web server log analysis. It highlights the system design and workflow used to process and aggregate massive log data using four distinct big-data processing engines: **MongoDB**, **Apache Hive**, **Apache Pig**, and **Hadoop MapReduce**, with all final analytical results securely stored in PostgreSQL.

---

## 2. Proposed Architecture and Workflow

### System Overview

The system implements a multi-pipeline ETL framework for processing large-scale web server logs. It focuses on transforming raw log data into a structured format, processing it in localized batches, and performing efficient aggregation across distributed backends.

The architecture is highly modular, explicitly separating the parsing, batching, storage, and query execution layers to permit identical logic to run across MongoDB, Hive, Pig, and MapReduce dynamically.

### End-to-End Data Flow

The data flows through the following stages:

1. **Input Layer** — Raw NASA log files are read from disk.
2. **Parsing Layer** — Logs are converted into structured records containing fields such as date, status code, and bytes.
3. **Batching Layer** — Records are grouped into fixed-size batches to enable scalable and parallel processing.
4. **Execution Backend** — The pipeline dynamically targets MongoDB, Hive, Pig, or MapReduce to execute the specific ETL workload.
5. **Processing Layer** — Aggregation queries are executed locally on the backend.
6. **Global Aggregation** — Partial results from all distributed batches are merged to produce final, globally consistent aggregated results.
7. **Output Layer** — Final results are enriched with metadata and stored in PostgreSQL for reporting.

### Design Principles

| Principle | Description |
|---|---|
| **Modularity** | Parsing, batching, and query execution are decoupled |
| **Scalability** | Batch-based processing handles massive data without memory overload |
| **Extensibility** | Pluggable architecture supporting distinct execution engines |
| **Consistency** | Strict parsing parity ensures identical results regardless of pipeline |

---

## 3. Parsing Strategy and Batching Approach

This section describes how raw NASA HTTP server log files are read, parsed into structured records, grouped into batches, and how malformed entries are efficiently handled.

### 3.1 Log Format and Structure

The input dataset consists of raw NASA HTTP server access log files:
- `NASA_access_log_Jul95`
- `NASA_access_log_Aug95`

Each line follows the **Combined Log Format (CLF)**.

**Sample log entry:**
```
199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245
```

### 3.2 Parsing Strategy

Parsing is universally handled to maintain 1:1 analytical parity. The system leverages Schema-on-Read, utilizing smart Regular Expressions (`regexp_extract`) to meticulously extract critical tokens while aggressively discarding trailing whitespaces and handling truncated requests (strict 3-part request tokenization).

### 3.3 Batching Approach

Large log files are never loaded entirely into memory. They are streamed and consumed in fixed-size batches, significantly bounded memory usage, preventing system crashes, and enforcing optimal distributed query execution across all the big data engines.

When generating batches, each batch receives a globally unique, monotonically incrementing `batchId` (resulting in ~117 batches for the full dataset using a 10,000 block size).

### 3.4 Handling Malformed Records

Malformed records (e.g., truncated text, bad HTTP protocols, invalid status codes) are aggressively trapped but not deleted. They are specifically filtered and safely stored in the metadata summaries to prevent them from corrupting the core analytics while remaining fully auditable.

---

## 4. Relational Reporting Database Schema

All execution pipelines output their aggregated analytics into a centralized PostgreSQL instance.

### Query 1 Reporting Table — `public.query_1`
Stores daily traffic summaries aggregated by log date and status code. Metrics include `request_count`, `total_bytes`, and the contributing `batch_id`.

### Query 2 Reporting Table — `public.query_2`
Stores the top requested resource paths. Metrics include `request_count`, `total_bytes`, and `distinct_hosts`.

### Query 3 Reporting Table — `public.query_3`
Stores hourly error analysis. Metrics include `error_request_count` (HTTP 400-599), `total_request_count`, `error_rate`, and `distinct_error_hosts`.

### Global Metadata Tables (`global_db`)
- `run_metadata`: Stores execution runtimes, pipeline names, and timestamps for high-level performance tracking.
- `batch_metadata`: Stores the exact granular chunking metrics, valid vs. malformed record counts, and average batch sizes.

---

## 5. Query Outputs and Demonstrations

All pipelines run three mandatory SQL queries on the cleaned dataset.

### Query 1: Daily Traffic Summary
Aggregates logs by **log date** and **status code**, computing total requests and total bytes transferred. Status code `200` dominates the traffic, representing primarily successful requests.

### Query 2: Top Requested Resources
Computes the **top 20 requested resource paths**, with total requests, total bytes, and distinct host counts. The analysis reveals static assets (logos, icons) naturally command the highest access frequencies across the highest unique client footprint.

### Query 3: Hourly Error Analysis
Aggregates logs by **log date** and **log hour**, grouping strict HTTP failures (400–599) to compute the exact error count, error rates, and distinct failed hosts. The output isolates specific network spike intervals and server misconfigurations.

---

## 6. Pipeline Performance Comparison

To evaluate the performance of the implemented ETL framework, execution statistics were collected from all four pipelines (MongoDB, Hive, Pig, and MapReduce).

| Pipeline | Record | Valid | Malformed | Batches | Execution Time (ms) |
|---|---|---|---|---|---|
| MongoDB | 3461613 | 3461612 | 1 | 35* | 136293 |
| Hive | 3461613 | 3461612 | 1 | 35* | 243748 |
| Pig | 3461613 | 3461612 | 1 | 35* | 145811 |
| MapReduce | 3461613 | 3461612 | 1 | 35* | 38180 |

*(Note: Batch configurations may vary based on `batch.size` settings, e.g., 35 batches vs 117 batches).*

**Observations:**
- **Consistency**: All four pipelines processed exactly 3,461,613 records and isolated 1 malformed record, demonstrating flawless 1:1 parity in parsing and validation across completely heterogeneous architectures.
- **Speed**: **MapReduce** achieved the fastest batch execution time (38,180 ms) due to raw distributed processing efficiencies. **MongoDB** was extremely fast in aggregation execution. **Pig** performed moderately with scripting overhead, and **Hive** required the highest runtime due to its complex SQL parsing, HDFS staging, and compilation overhead.

---

## 7. Runtime Module

The framework features a final centralized orchestration reporting feature triggered via the interactive CLI menu (`Option 5 - Run All Pipelines`). 

The `FinalReportService` queries the PostgreSQL `global_db` to dynamically construct and print two beautifully formatted ASCII tables directly to the terminal:
1. **Pipeline Execution Summary** (`run_metadata`): High-level system execution times and completion statuses.
2. **Batch Processing Details** (`batch_metadata`): Granular statistics on valid records, malformed records, and batch distribution.

This centralized view acts as the ultimate validation layer for the entire project.

---

## 8. Conclusion

This project successfully engineered and implemented a robust Multi-Pipeline NoSQL ETL framework explicitly designed for large-scale Big Data analytics.

The framework actively demonstrates:
- Scalable batch-based processing.
- Reusable shared parsing logic deployed uniformly.
- Extensible multi-module Maven design.
- Sophisticated malformed record containment.
- Centralized reporting and benchmarking.

By completing exact parity across **MongoDB**, **Hive**, **Pig**, and **MapReduce**, this project vividly illustrates the exact performance trade-offs, architecture strengths, and unique design patterns required to succeed in modern distributed data ecosystems.
