package com.example.mapReduce.jobs.batchProcessing;

import com.example.config.ConfigReader;
import com.example.model.ParsedLog;
import com.example.util.LogParser;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class BatchProcessingMapper extends
        Mapper<Object, Text, NullWritable, Text> {

    // =====================================
    // COUNTERS
    // =====================================

    public enum LogCounters {
        TOTAL_VALID,
        TOTAL_MALFORMED,
        TOTAL_RECORDS,
        TOTAL_BATCHES
    }

    private MultipleOutputs<NullWritable, Text> multipleOutputs;

    private String mapperId;

    @Override
    protected void setup(Context context) {

        multipleOutputs =
                new MultipleOutputs<>(context);

        mapperId =
                context
                        .getTaskAttemptID()
                        .getTaskID()
                        .toString();

        context.getCounter(
                LogCounters.TOTAL_BATCHES
        ).increment(1);

        System.out.println(
                "\nMapper started: " + mapperId
        );
    }

    @Override
    protected void map(
            Object key,
            Text value,
            Context context
    ) throws IOException, InterruptedException {

        String line =
                value.toString().trim();

        if (line.isEmpty()) {
            return;
        }

        context.getCounter(
                LogCounters.TOTAL_RECORDS
        ).increment(1);

        // =====================================
        // PARSE USING COMMON UTIL
        // =====================================

        ParsedLog log =
                LogParser.parse(
                        line,
                        0
                );

        // =====================================
        // MALFORMED
        // =====================================

        if (log.isMalformed()) {

            context.getCounter(
                    LogCounters.TOTAL_MALFORMED
            ).increment(1);

            multipleOutputs.write(
                    NullWritable.get(),
                    new Text(
                            mapperId + "\t" + line
                    ),
                    ConfigReader.get(
                            "hdfs.output.malformed.path"
                    ) + "/malformed"
            );

            return;
        }

        // =====================================
        // VALID
        // =====================================

        context.getCounter(
                LogCounters.TOTAL_VALID
        ).increment(1);

        String parsedLine =

                log.getHost() + "\t" +
                        log.getRawTimestamp() + "\t" +
                        log.getDate() + "\t" +
                        log.getHour() + "\t" +
                        log.getMethod() + "\t" +
                        log.getPath() + "\t" +
                        log.getProtocol() + "\t" +
                        log.getStatus() + "\t" +
                        log.getBytes();

        multipleOutputs.write(
                NullWritable.get(),
                new Text(parsedLine),
                ConfigReader.get(
                        "hdfs.output.filtered.path"
                ) + "/filtered"
        );
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {

        System.out.println(
                "Mapper completed: " + mapperId
        );

        multipleOutputs.close();
    }
}