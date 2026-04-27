package com.example.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BatchResult {

    private int batchId;

    private int totalRecords;
    private int malformedRecords;
    private int validRecords;

    private List<ParsedLog> parsedLogs;
}