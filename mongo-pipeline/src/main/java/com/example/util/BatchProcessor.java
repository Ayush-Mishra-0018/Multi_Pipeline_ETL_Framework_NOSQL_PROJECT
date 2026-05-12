package com.example.util;

import com.example.model.BatchResult;
import com.example.model.MalformedRecord;
import com.example.model.ParsedLog;

import java.util.ArrayList;
import java.util.List;

public final class BatchProcessor {


    private BatchProcessor() {
    }

    public static BatchResult processBatch(
            List<String> rawLines,
            int batchId
    ) {

        List<ParsedLog> parsedLogs =
                new ArrayList<>(rawLines.size());

        List<MalformedRecord> malformedRecords =
                new ArrayList<>();

        int malformedCount = 0;

        for (String line : rawLines) {

            ParsedLog log =
                    LogParser.parse(
                            line,
                            batchId
                    );

            if (log.isMalformed()) {

                malformedCount++;

                malformedRecords.add(
                        MalformedRecord.builder()
                                .batchId(batchId)
                                .line(line)
                                .build()
                );
            }

            parsedLogs.add(log);
        }

        return BatchResult.builder()
                .batchId(batchId)
                .totalRecords(rawLines.size())
                .malformedRecords(malformedCount)
                .validRecords(rawLines.size() - malformedCount)
                .parsedLogs(parsedLogs)
                .malformedLogs(malformedRecords)
                .build();
    }
}