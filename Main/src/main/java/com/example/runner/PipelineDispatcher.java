package com.example.runner;


import com.example.hive.RunReportHive;
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
                        "\nRunning MongoDB Pipeline..."
                );

                RunReportMongo.reporting(
                        queries
                );

                break;

            case 2:

                RunReportHive.reporting(queries);

                break;

            case 3:

                RunReportPig.reporting(queries);

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