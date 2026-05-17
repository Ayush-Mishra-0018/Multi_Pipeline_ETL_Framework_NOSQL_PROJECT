package com.example.runner;

import com.example.hive.RunReportHive;
import com.example.mapReduce.RunReportMapReduce;
import com.example.mongo.RunReportMongo;
import com.example.pig.RunReportPig;

import java.util.List;

public final class PipelineDispatcher {

    private PipelineDispatcher() {
    }

    public static void dispatch(
            int pipelineChoice,
            List<Integer> queries
    ) {

        switch (pipelineChoice) {

            case 1:

                System.out.println(
                        "\n=================================================="
                );

                System.out.println(
                        "Running MongoDB Pipeline..."
                );

                System.out.println(
                        "=================================================="
                );

                RunReportMongo.reporting(
                        queries
                );

                break;

            case 2:

                System.out.println(
                        "\n=================================================="
                );

                System.out.println(
                        "Running Hive Pipeline..."
                );

                System.out.println(
                        "=================================================="
                );

                RunReportHive.reporting(
                        queries
                );

                break;

            case 3:

                System.out.println(
                        "\n=================================================="
                );

                System.out.println(
                        "Running Pig Pipeline..."
                );

                System.out.println(
                        "=================================================="
                );

                RunReportPig.reporting(
                        queries
                );

                break;

            case 4:

                System.out.println(
                        "\n=================================================="
                );

                System.out.println(
                        "Running MapReduce Pipeline..."
                );

                System.out.println(
                        "=================================================="
                );

                RunReportMapReduce.reporting(
                        queries
                );

                break;

            case 5:

                runAllPipelines(queries);

                break;

            default:

                System.out.println(
                        "\nInvalid pipeline choice."
                );
        }
    }

    private static void runAllPipelines(
            List<Integer> queries
    ) {

        System.out.println(
                "\n=================================================="
        );

        System.out.println(
                "RUNNING ALL PIPELINES"
        );

        System.out.println(
                "=================================================="
        );

        // =====================================================
        // MongoDB
        // =====================================================

        System.out.println(
                "\n>>> Starting MongoDB Pipeline..."
        );

        RunReportMongo.reporting(
                queries
        );

        // =====================================================
        // Hive
        // =====================================================

        System.out.println(
                "\n>>> Starting Hive Pipeline..."
        );

        RunReportHive.reporting(
                queries
        );

        // =====================================================
        // Pig
        // =====================================================

        System.out.println(
                "\n>>> Starting Pig Pipeline..."
        );

        RunReportPig.reporting(
                queries
        );

        // =====================================================
        // MapReduce
        // =====================================================

        System.out.println(
                "\n>>> Starting MapReduce Pipeline..."
        );

        RunReportMapReduce.reporting(
                queries
        );

        System.out.println(
                "\n=================================================="
        );

        System.out.println(
                "ALL PIPELINES COMPLETED"
        );

        System.out.println(
                "=================================================="
        );
    }
}