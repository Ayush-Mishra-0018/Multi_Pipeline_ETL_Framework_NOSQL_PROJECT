package com.example.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BatchStatistics {

    private long totalRecordsProcessed;

    private long totalValid;

    private long totalMalformed;

    private long totalBatches;

    private double avgBatchSize;

    private long totalTime;
}