package com.example.mapReduce.dataSetup;

import com.example.config.ConfigReader;
import com.example.mapReduce.jobs.batchProcessing.BatchProcessingDriver;
import com.example.mapReduce.service.HdfsFileUploader;
import com.example.model.MalformedRecord;
import com.example.model.PipelineExecutionResult;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import java.util.ArrayList;
import java.util.List;

public class MapReduceDataSetupExecutor {
    private static int extractBatchId(String mapperId) {

        try {

            String[] tokens =
                    mapperId.split("_");

            return Integer.parseInt(
                    tokens[tokens.length - 1]
            );

        } catch (Exception e) {

            return -1;
        }
    }

    public static PipelineExecutionResult execute() {

        try {

            // =====================================
            // UPLOAD INPUT FILES TO HDFS
            // =====================================

            HdfsFileUploader.uploadInputDataIfMissing();

            // =====================================
            // RUN MAPREDUCE JOB
            // =====================================

            boolean success =
                    BatchProcessingDriver.runJob();

            if (!success) {

                System.out.println(
                        "MapReduce job failed."
                );

                return null;
            }

            // =====================================
            // HDFS CONFIG
            // =====================================

            Configuration conf =
                    new Configuration();

            conf.set(
                    "fs.defaultFS",
                    ConfigReader.get("hdfs.uri")
            );

            FileSystem fs =
                    FileSystem.get(conf);

            // =====================================
            // READ STATS FILE
            // =====================================

            long executionTime = 0;

            Path statsFile =
                    new Path(
                            ConfigReader.get(
                                    "hdfs.output.stats.path"
                            ) + "/stats.txt"
                    );

            if (fs.exists(statsFile)) {

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

                        if (line.startsWith("totalTime=")) {

                            String value =
                                    line.split("=")[1]
                                            .replace("ms", "")
                                            .trim();

                            executionTime =
                                    Long.parseLong(value);
                        }
                    }
                }
            }

            // =====================================
            // READ MALFORMED RECORDS
            // =====================================

            List<MalformedRecord> malformedRecords =
                    new ArrayList<>();

            Path malformedDir =
                    new Path(
                            ConfigReader.get(
                                    "hdfs.output.malformed.path"
                            )
                    );

            if (fs.exists(malformedDir)) {

                RemoteIterator<LocatedFileStatus> files =
                        fs.listFiles(
                                malformedDir,
                                true
                        );

                while (files.hasNext()) {

                    LocatedFileStatus file =
                            files.next();

                    String fileName =
                            file.getPath().getName();

                    if (!fileName.startsWith("malformed")) {
                        continue;
                    }

                    try (
                            FSDataInputStream in =
                                    fs.open(file.getPath());

                            BufferedReader reader =
                                    new BufferedReader(
                                            new InputStreamReader(in)
                                    )
                    ) {

                        String line;

                        while ((line = reader.readLine()) != null) {

                            String[] parts =
                                    line.split("\t", 2);

                            String mapperId = parts[0];
                            String malformedLine = parts[1];

                            malformedRecords.add(

                                    MalformedRecord.builder()
                                            .batchId(
                                                    extractBatchId(mapperId)
                                            )
                                            .line(malformedLine)
                                            .build()
                            );
                        }
                    }
                }
            }

            fs.close();

            // =====================================
            // RETURN RESULT
            // =====================================

            return PipelineExecutionResult
                    .builder()
                    .executionTime(executionTime)
                    .malformedRecords(malformedRecords)
                    .build();

        } catch (Exception e) {

            e.printStackTrace();
        }

        return null;
    }

}