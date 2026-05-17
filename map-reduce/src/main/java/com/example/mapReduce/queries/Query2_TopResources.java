package com.example.mapReduce.queries;

import com.example.config.ConfigReader;
import com.example.postgres.service.PostgresInsertService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import java.util.*;

public class Query2_TopResources {

    public static void run() {

        System.out.println(
                "\n\n Top Resources Global \n\n"
        );

        try {

            // =====================================================
            // HADOOP CONFIG
            // =====================================================

            Configuration conf =
                    new Configuration();

            conf.addResource(
                    new Path(
                            ConfigReader.get(
                                    "hadoop.core.site"
                            )
                    )
            );

            conf.addResource(
                    new Path(
                            ConfigReader.get(
                                    "hadoop.hdfs.site"
                            )
                    )
            );

            conf.addResource(
                    new Path(
                            ConfigReader.get(
                                    "hadoop.mapred.site"
                            )
                    )
            );

            conf.addResource(
                    new Path(
                            ConfigReader.get(
                                    "hadoop.yarn.site"
                            )
                    )
            );

            // =====================================================
            // JOB
            // =====================================================
            conf.set(

                    "mapreduce.job.jar",

                    ConfigReader.get("mapreduce.job.jar")

            );
            Job job =
                    Job.getInstance(
                            conf,
                            "Query2 Top Resources"
                    );

            job.setJarByClass(
                    Query2_TopResources.class
            );

            // =====================================================
            // MAPPER / REDUCER
            // =====================================================

            job.setMapperClass(
                    Query2Mapper.class
            );

            job.setReducerClass(
                    Query2Reducer.class
            );

            // IMPORTANT
            // ensures global aggregation

            job.setNumReduceTasks(1);

            // =====================================================
            // TYPES
            // =====================================================

            job.setMapOutputKeyClass(
                    Text.class
            );

            job.setMapOutputValueClass(
                    Text.class
            );

            job.setOutputKeyClass(
                    NullWritable.class
            );

            job.setOutputValueClass(
                    Text.class
            );

            // =====================================================
            // INPUT / OUTPUT
            // =====================================================

            String inputPath =
                    ConfigReader.get(
                            "hdfs.output.filtered.path"
                    );

            String outputPath =
                    ConfigReader.get(
                            "hdfs.output.path"
                    ) + "/query_2";

            Path outputDir =
                    new Path(outputPath);

            FileSystem fs =
                    FileSystem.get(conf);

            // delete previous output

            if (fs.exists(outputDir)) {

                fs.delete(outputDir, true);
            }

            FileInputFormat.addInputPath(
                    job,
                    new Path(inputPath)
            );

            FileOutputFormat.setOutputPath(
                    job,
                    outputDir
            );

            // =====================================================
            // RUN JOB
            // =====================================================

            boolean success =
                    job.waitForCompletion(true);

            if (!success) {

                throw new RuntimeException(
                        "Query2 failed"
                );
            }

            // =====================================================
            // READ OUTPUT
            // =====================================================

            List<Map<String, Object>> rows =
                    new ArrayList<>();

            RemoteIterator<LocatedFileStatus> files =
                    fs.listFiles(outputDir, false);

            while (files.hasNext()) {

                LocatedFileStatus file =
                        files.next();

                String fileName =
                        file.getPath().getName();

                if (!fileName.startsWith("part-")) {
                    continue;
                }

                BufferedReader br =
                        new BufferedReader(
                                new InputStreamReader(
                                        fs.open(file.getPath())
                                )
                        );

                String line;

                while ((line = br.readLine()) != null) {

                    /*
                        FORMAT:

                        path \t count \t totalBytes
                        \t distinctHosts \t batches
                     */

                    String[] parts =
                            line.split("\\t");

                    if (parts.length != 5) {
                        continue;
                    }

                    Map<String, Object> row =
                            new LinkedHashMap<>();

                    row.put(
                            "resource_path",
                            parts[0]
                    );

                    row.put(
                            "request_count",
                            Integer.parseInt(parts[1])
                    );

                    row.put(
                            "total_bytes",
                            Long.parseLong(parts[2])
                    );

                    row.put(
                            "distinct_hosts",
                            Integer.parseInt(parts[3])
                    );

                    row.put(
                            "batch_id",
                            parts[4]
                    );

                    rows.add(row);
                }

                br.close();
            }

            // =====================================================
            // GLOBAL SORT
            // =====================================================

            rows.sort(
                    Comparator.comparing(
                                    (Map<String, Object> d) ->
                                            (Integer) d.get("request_count")
                            )
                            .reversed()
                            .thenComparing(
                                    d ->
                                            (String) d.get("resource_path")
                            )
            );

            // =====================================================
            // TOP 20
            // =====================================================

            if (rows.size() > 20) {

                rows =
                        new ArrayList<>(
                                rows.subList(0, 20)
                        );
            }

            // =====================================================
            // ASCENDING DISPLAY
            // =====================================================

            rows.sort(
                    Comparator.comparing(
                                    (Map<String, Object> d) ->
                                            (Integer) d.get("request_count")
                            )
                            .thenComparing(
                                    d ->
                                            (String) d.get("resource_path")
                            )
            );

            // =====================================================
            // INSERT POSTGRES
            // =====================================================

            System.out.println(
                    "Rows to insert: "
                            + rows.size()
            );

            PostgresInsertService.Insert(
                    "mapreduce",
                    "query_2",
                    rows
            );

            // =====================================================
            // PRINT
            // =====================================================

            System.out.printf(
                    "%-50s | %-14s | %-14s | %-15s | %-10s%n",
                    "resource_path",
                    "request_count",
                    "total_bytes",
                    "distinct_hosts",
                    "batches"
            );

            System.out.println(
                    "----------------------------------------------------------------------------------------------------------------------------------------------------------------"
            );

            for (Map<String, Object> row : rows) {

                System.out.printf(
                        "%-50s | %-14d | %-14d | %-15d | %-10s%n",
                        row.get("resource_path"),
                        row.get("request_count"),
                        row.get("total_bytes"),
                        row.get("distinct_hosts"),
                        row.get("batch_id")
                );
            }

        } catch (Exception e) {

            e.printStackTrace();

            throw new RuntimeException(e);
        }
    }

    // =====================================================
    // MAPPER
    // =====================================================

    public static class Query2Mapper
            extends Mapper<
            LongWritable,
            Text,
            Text,
            Text> {

        @Override
        protected void map(
                LongWritable key,
                Text value,
                Context context
        ) {

            try {

                String line =
                        value.toString().trim();

                if (line.isEmpty()) {
                    return;
                }

                /*
                    FILTERED FORMAT:

                    host	rawTimestamp	date	hour	method
                    path	protocol	status	bytes
                 */

                String[] parts =
                        line.split("\\t", -1);

                // strict validation

                if (parts.length != 9) {

                    System.out.println(
                            "Skipping malformed line: "
                                    + line
                    );

                    return;
                }

                String host =
                        parts[0].trim();

                String path =
                        parts[5].trim();

                if (path.isEmpty()) {
                    return;
                }

                long bytes = 0;

                String byteField =
                        parts[8].trim();

                if (!byteField.equals("-")
                        && !byteField.isEmpty()) {

                    try {

                        bytes =
                                Long.parseLong(
                                        byteField
                                );

                    } catch (Exception e) {

                        return;
                    }
                }

                // =====================================================
                // BATCH ID FROM FILE NAME
                // =====================================================

                FileSplit split =
                        (FileSplit)
                                context.getInputSplit();

                String fileName =
                        split.getPath().getName();

                /*
                    Example:

                    filtered-m-00112
                 */

                int batchId;

                try {

                    String number =
                            fileName.substring(
                                    fileName.lastIndexOf("-") + 1
                            );

                    batchId =
                            Integer.parseInt(number);

                } catch (Exception e) {

                    batchId = 0;
                }

                /*
                    VALUE FORMAT:

                    bytes|host|batchId
                 */

                String outValue =
                        bytes
                                + "|"
                                + host
                                + "|"
                                + batchId;

                context.write(
                        new Text(path),
                        new Text(outValue)
                );

            } catch (Exception e) {

                System.out.println(
                        "Mapper Error: "
                                + e.getMessage()
                );
            }
        }
    }

    // =====================================================
    // REDUCER
    // =====================================================

    public static class Query2Reducer
            extends Reducer<
            Text,
            Text,
            NullWritable,
            Text> {

        @Override
        protected void reduce(
                Text key,
                Iterable<Text> values,
                Context context
        ) {

            try {

                int requestCount = 0;

                long totalBytes = 0;

                Set<String> hosts =
                        new HashSet<>();

                Set<Integer> batches =
                        new TreeSet<>();

                for (Text value : values) {

                    String[] parts =
                            value.toString().split("\\|");

                    if (parts.length != 3) {
                        continue;
                    }

                    requestCount++;

                    totalBytes +=
                            Long.parseLong(parts[0]);

                    hosts.add(
                            parts[1].trim()
                    );

                    batches.add(
                            Integer.parseInt(parts[2])
                    );
                }

                // =====================================================
                // BUILD BATCH STRING
                // =====================================================

                String batchString =
                        String.join(
                                "+",
                                batches.stream()
                                        .map(String::valueOf)
                                        .toArray(String[]::new)
                        );

                /*
                    OUTPUT FORMAT:

                    path	count	bytes	distinctHosts	batches
                 */

                String output =
                        key.toString()
                                + "\t"
                                + requestCount
                                + "\t"
                                + totalBytes
                                + "\t"
                                + hosts.size()
                                + "\t"
                                + batchString;

                context.write(
                        NullWritable.get(),
                        new Text(output)
                );

            } catch (Exception e) {

                System.out.println(
                        "Reducer Error: "
                                + e.getMessage()
                );
            }
        }
    }
}