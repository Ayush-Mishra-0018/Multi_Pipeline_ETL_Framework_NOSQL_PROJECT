package com.example;

import com.example.input.UserInputHandler;
import com.example.menu.ConsoleMenu;
import com.example.postgres.service.PostgresSchemaInitializer;
import com.example.runner.PipelineDispatcher;

import java.util.List;

public class Main {

    public static void main(String[] args) {

        ConsoleMenu.printWelcome();

        ConsoleMenu.printPipelines();

        int pipelineChoice =
                UserInputHandler.getPipelineChoice();

        ConsoleMenu.printQueries();

        List<Integer> queries =
                UserInputHandler.getQueries();
        PostgresSchemaInitializer.initializeGlobal();

        PipelineDispatcher.dispatch(
                pipelineChoice,
                queries
        );
    }
}