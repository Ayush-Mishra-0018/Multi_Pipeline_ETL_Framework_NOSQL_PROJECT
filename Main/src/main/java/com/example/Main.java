package com.example;

import com.example.postgres.service.PostgresSchemaInitializer;
import com.example.runner.PipelineDispatcher;
import com.example.service.FinalReportService;
import com.example.setup.ApplicationSetup;

public class Main {

    public static void main(String[] args) {

        ApplicationSetup setup =
                ApplicationSetup.initialize();

        PostgresSchemaInitializer.initializeGlobal();

        PipelineDispatcher.dispatch(
                setup.getPipelineChoice(),
                setup.getQueries()
        );

        if (setup.getPipelineChoice() == 5) {
            FinalReportService.generateFinalReport();
        }
    }
}