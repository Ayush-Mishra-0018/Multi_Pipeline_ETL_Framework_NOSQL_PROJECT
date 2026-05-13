package com.example.mapReduce;

import com.example.mapReduce.dataSetup.MapReduceDataSetupExecutor;
import com.example.mapReduce.service.StatsReaderService;

import com.example.model.BatchStatistics;
import com.example.model.MalformedRecord;
import com.example.model.PipelineExecutionResult;

import java.util.List;

public class RunReportMapReduce {

    public static void reporting(
            List<Integer> queries
    ) {

        try {

            // =====================================
            // RUN COMPLETE PIPELINE
            // =====================================

            PipelineExecutionResult result =
                    MapReduceDataSetupExecutor.execute();

            // =====================================
            // READ STATS
            // =====================================

            BatchStatistics stats =
                    StatsReaderService.readStats();

            // =====================================
            // PRINT EXECUTION RESULT
            // =====================================

            System.out.println(
                    "\n================================="
            );

            System.out.println(
                    "MAPREDUCE REPORT"
            );

            System.out.println(
                    "================================="
            );

            // =====================================
            // PIPELINE RESULT
            // =====================================

            System.out.println(
                    "\nExecution Time: "
                            + result.getExecutionTime()
                            + " ms"
            );

            // =====================================
            // STATS
            // =====================================

            System.out.println(
                    "\n========== STATS =========="
            );

            System.out.println(
                    "Total Records Processed: "
                            + stats.getTotalRecordsProcessed()
            );

            System.out.println(
                    "Total Valid Records: "
                            + stats.getTotalValid()
            );

            System.out.println(
                    "Total Malformed Records: "
                            + stats.getTotalMalformed()
            );

            System.out.println(
                    "Total Batches: "
                            + stats.getTotalBatches()
            );

            System.out.println(
                    "Average Batch Size: "
                            + stats.getAvgBatchSize()
            );

            System.out.println(
                    "Total Processing Time: "
                            + stats.getTotalTime()
                            + " ms"
            );

            // =====================================
            // MALFORMED RECORDS
            // =====================================

            List<MalformedRecord> malformedRecords =
                    result.getMalformedRecords();

            System.out.println(
                    "\n========== MALFORMED RECORDS =========="
            );

            System.out.println(
                    "Count: "
                            + malformedRecords.size()
            );

            for (MalformedRecord record : malformedRecords) {

                System.out.println(
                        "BatchId: "
                                + record.getBatchId()
                                + " | Line: "
                                + record.getLine()
                );
            }

            System.out.println(
                    "\n================================="
            );

            System.out.println("\n\n Naveed please do your work now\n\n");

        } catch (Exception e) {

            e.printStackTrace();
        }
    }
}