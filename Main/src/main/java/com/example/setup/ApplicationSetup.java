package com.example.setup;

import com.example.config.ConfigReader;
import com.example.config.PropertyUpdater;
import com.example.input.UserInputHandler;
import com.example.menu.ConsoleMenu;

import java.util.List;

public final class ApplicationSetup {

    private final int pipelineChoice;

    private final List<Integer> queries;

    private ApplicationSetup(
            int pipelineChoice,
            List<Integer> queries
    ) {

        this.pipelineChoice =
                pipelineChoice;

        this.queries =
                queries;
    }

    public static ApplicationSetup initialize() {

        ConsoleMenu.printWelcome();

        // =====================================================
        // PIPELINE
        // =====================================================

        ConsoleMenu.printPipelines();

        int pipelineChoice =
                UserInputHandler.getPipelineChoice();

        // =====================================================
        // QUERIES
        // =====================================================

        ConsoleMenu.printQueries();

        List<Integer> queries =
                UserInputHandler.getQueries();

        // =====================================================
        // BATCH SIZE
        // =====================================================

        String currentBatchSize =
                ConfigReader.get(
                        "batch.size",
                        "1000"
                );

        System.out.println(
                "\nCurrent batch.size = " +
                        currentBatchSize
        );

        int batchSize =
                UserInputHandler.getBatchSize(
                        Integer.parseInt(currentBatchSize)
                );

        // =====================================================
        // UPDATE PROPERTY
        // =====================================================

        if (batchSize != Integer.parseInt(currentBatchSize)) {

            PropertyUpdater.updateProperty(
                    "batch.size",
                    String.valueOf(batchSize)
            );
        }

        return new ApplicationSetup(
                pipelineChoice,
                queries
        );
    }

    public int getPipelineChoice() {
        return pipelineChoice;
    }

    public List<Integer> getQueries() {
        return queries;
    }
}