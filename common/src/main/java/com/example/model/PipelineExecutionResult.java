package com.example.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PipelineExecutionResult {

    private long executionTime;

    private List<MalformedRecord> malformedRecords;

    private int totalBatches;

    private Map<String, Object> metadata;
}