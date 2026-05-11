package com.example.runner;


import com.example.mongo.RunReportMongo;

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
                        "\nRunning MongoDB Pipeline..."
                );

                RunReportMongo.reporting(
                        queries
                );

                break;

            case 2:

                System.out.println(
                        "\nHive pipeline not implemented yet."
                );

                break;

            case 3:

                System.out.println(
                        "\nPig pipeline not implemented yet."
                );

                break;

            case 4:

                System.out.println(
                        "\nMapReduce pipeline not implemented yet."
                );

                break;

            default:

                System.out.println(
                        "\nInvalid pipeline choice."
                );
        }
    }
}