package com.example.pig;

import com.example.config.ConfigReader;
import com.example.model.MalformedRecord;
import com.example.model.PipelineExecutionResult;
import com.example.pig.dataSetup.PigDataSetupExecutor;
import com.example.pig.runner.PigQueryRunner;

import java.util.List;

public class RunReportPig {
    // test commit for new branch (pig-pipeline-refinements)
    public static void reporting(
            List<Integer> queries
    ) {
        try {
            System.out.println("\nRunning Pig Pipeline Data Setup...");
            PipelineExecutionResult executionResult = PigDataSetupExecutor.execute();

            long pipelineRuntime = executionResult.getExecutionTime();
            System.out.println("Pig data setup completed in " + pipelineRuntime + " ms");

            List<MalformedRecord> malformedRecords = executionResult.getMalformedRecords();

            // Query execution
            long queryRuntime = PigQueryRunner.runQueries(queries);

            long totalRuntime = pipelineRuntime + queryRuntime;

            int batchSize =
                    Integer.parseInt(
                            ConfigReader.get(
                                    "batch.size"
                            )
                    );

            com.example.postgres.service.PostgresInsertService.insertGlobalMetadataMap(
                    "pig",
                    queries,
                    totalRuntime,
                    executionResult.getMetadata()
            );

            com.example.postgres.service.PostgresInsertService.insertMalformed("pig", malformedRecords);


        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
