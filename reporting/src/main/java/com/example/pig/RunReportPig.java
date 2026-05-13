package com.example.pig;

import com.example.model.PipelineExecutionResult;
import com.example.pig.dataSetup.PigDataSetupExecutor;

import java.util.List;

public class RunReportPig {
    public static void reporting(
            List<Integer> queries
    ) {
        System.out.println("\nRunning Pig Pipeline Data Setup...");
        PipelineExecutionResult executionResult = PigDataSetupExecutor.execute();
        
        System.out.println("Pig data setup completed in " + executionResult.getExecutionTime() + " ms");
        
        // Query execution goes here
    }
}
