package com.example.mapReduce.service;

import com.example.config.ConfigReader;
import com.example.model.BatchStatistics;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public final class StatsReaderService {

    private StatsReaderService() {
    }

    public static BatchStatistics readStats()
            throws Exception {

        Configuration conf =
                new Configuration();

        conf.set(
                "fs.defaultFS",
                ConfigReader.get("hdfs.uri")
        );

        FileSystem fs =
                FileSystem.get(conf);

        Path statsFile =
                new Path(
                        ConfigReader.get(
                                "hdfs.output.stats.path"
                        ) + "/stats.txt"
                );

        BatchStatistics stats =
                new BatchStatistics();

        try (
                FSDataInputStream in =
                        fs.open(statsFile);

                BufferedReader reader =
                        new BufferedReader(
                                new InputStreamReader(in)
                        )
        ) {

            String line;

            while ((line = reader.readLine()) != null) {

                String[] parts =
                        line.split("=");

                if (parts.length != 2) {
                    continue;
                }

                String key =
                        parts[0].trim();

                String value =
                        parts[1]
                                .replace("ms", "")
                                .trim();

                switch (key) {

                    case "totalRecordsProcessed":

                        stats.setTotalRecordsProcessed(
                                Long.parseLong(value)
                        );

                        break;

                    case "totalValid":

                        stats.setTotalValid(
                                Long.parseLong(value)
                        );

                        break;

                    case "totalMalformed":

                        stats.setTotalMalformed(
                                Long.parseLong(value)
                        );

                        break;

                    case "totalBatches":

                        stats.setTotalBatches(
                                Long.parseLong(value)
                        );

                        break;

                    case "avgBatchSize":

                        stats.setAvgBatchSize(
                                Double.parseDouble(value)
                        );

                        break;

                    case "totalTime":

                        stats.setTotalTime(
                                Long.parseLong(value)
                        );

                        break;
                }
            }
        }

        fs.close();

        return stats;
    }
}