package com.example.pig;

import com.example.config.ConfigReader;
import com.example.model.MalformedRecord;
import com.example.model.PipelineExecutionResult;
import com.example.pig.dataSetup.PigDataSetupExecutor;
import com.example.pig.runner.PigQueryRunner;

import com.example.pig.queries.Query1_DailyTraffic_Global;
import com.example.pig.queries.Query2_TopResources;
import com.example.pig.queries.Query3_HourlyErrorAnalysis;

import java.util.ArrayList;
import java.util.List;

class BatchExecResult {
    int batchSize;
    long dataSetupTime;
    long q1Time;
    long q2Time;
    long q3Time;

    public BatchExecResult(int batchSize, long dataSetupTime, long q1Time, long q2Time, long q3Time) {
        this.batchSize = batchSize;
        this.dataSetupTime = dataSetupTime;
        this.q1Time = q1Time;
        this.q2Time = q2Time;
        this.q3Time = q3Time;
    }

    @Override
    public String toString() {
        String res = "Batch Execution Results for batch size = " + batchSize + ":\n" +
                        "\n    data setup time: " + dataSetupTime +
                        "\n    q1 time: " + q1Time+
                        "\n    q2 time: " + q2Time+
                        "\n    q3 time: " + q3Time;
        return res;
    }
}

public class RunPigMultiBatch {
    public static void main(String[] args) {
        List<Integer> batchSizes = List.of(10000, 20000, 25000, 50000, 75000, 100000, 125000);
        List<BatchExecResult> batchResults = new ArrayList<>();

        for (int batchSize : batchSizes) {
            try {
                PipelineExecutionResult executionResult = PigDataSetupExecutor.execute(batchSize);
                System.out.println("Data setup for Pig pipeline with batch size " + batchSize +
                        " completed in " + executionResult.getExecutionTime());

                // query 1
                long q1_start = System.currentTimeMillis();
                Query1_DailyTraffic_Global.run();
                long q1_time = System.currentTimeMillis() - q1_start;

                // query 2
                long q2_start = System.currentTimeMillis();
                Query2_TopResources.run();
                long q2_time = System.currentTimeMillis() - q2_start;

                // query 3
                long q3_start = System.currentTimeMillis();
                Query3_HourlyErrorAnalysis.run();
                long q3_time = System.currentTimeMillis() - q3_start;

                BatchExecResult batchExecResult =
                        new BatchExecResult(batchSize, executionResult.getExecutionTime(), q1_time, q2_time, q3_time);
                batchResults.add(batchExecResult);
            }
            catch (Exception e) {
                System.out.println("Failed to run on batch size of " + batchSize);
                System.out.println(e.getMessage());
            }
        }

        System.out.println("\n\n\n\n\n");
        System.out.println("==========================================");
        for (BatchExecResult batchResult : batchResults) {
            System.out.println(batchResult);
        }
        System.out.println("==========================================");
    }
}
