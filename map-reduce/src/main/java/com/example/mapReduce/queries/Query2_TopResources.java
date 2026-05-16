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

        /*
            FINAL AGGREGATED DATA

            key:
                resource_path
         */

        Map<String, Map<String, Object>> finalMap =
                new HashMap<>();

        /*
            TRACK BATCH IDS
         */

        Map<String, Set<Integer>> batchTracker =
                new HashMap<>();

        try {

            // =====================================
            // HADOOP CONFIGURATION
            // =====================================

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

            // =====================================
            // CREATE JOB
            // =====================================

            conf.set(

                    "mapreduce.job.jar",

                    ConfigReader.get("mapreduce.job.jar")

            );

// =====================================

// CREATE JOB

// =====================================

            Job job =

                    Job.getInstance(

                            conf,

                            "Query2 Top Resources"

                    );

            job.setJarByClass(
                    Query2_TopResources.class
            );

            // =====================================
            // MAPPER + REDUCER
            // =====================================

            job.setMapperClass(
                    Query2Mapper.class
            );

            job.setReducerClass(
                    Query2Reducer.class
            );

            // =====================================
            // OUTPUT TYPES
            // =====================================

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

            // =====================================
            // INPUT / OUTPUT
            // =====================================

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

            // =====================================
            // DELETE OLD OUTPUT
            // =====================================

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

            // =====================================
            // RUN JOB
            // =====================================

            boolean success =
                    job.waitForCompletion(true);

            if (!success) {

                throw new RuntimeException(
                        "Query2 MapReduce job failed"
                );
            }

            // =====================================
            // READ REDUCER OUTPUT
            // =====================================

            RemoteIterator<LocatedFileStatus> files =
                    fs.listFiles(outputDir, false);

            while (files.hasNext()) {

                LocatedFileStatus file =
                        files.next();

                String fileName =
                        file.getPath().getName();

                System.out.println(
                        "Found file: " + fileName
                );

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

                        path,count,totalBytes,distinctHosts,batches
                     */

                    String[] parts =
                            line.split("\\t");

                    if (parts.length < 5) {
                        continue;
                    }

                    String path =
                            parts[0];

                    int count =
                            Integer.parseInt(parts[1]);

                    long totalBytes =
                            Long.parseLong(parts[2]);

                    int distinctHosts =
                            Integer.parseInt(parts[3]);

                    String batchString =
                            parts[4];

                    Map<String, Object> rowData =
                            new HashMap<>();

                    rowData.put(
                            "resource_path",
                            path
                    );

                    rowData.put(
                            "request_count",
                            count
                    );

                    rowData.put(
                            "total_bytes",
                            totalBytes
                    );

                    rowData.put(
                            "distinct_hosts",
                            distinctHosts
                    );

                    finalMap.put(
                            path,
                            rowData
                    );

                    // =====================================
                    // TRACK BATCH IDS
                    // =====================================

                    Set<Integer> batches =
                            new HashSet<>();

                    for (String s :
                            batchString.split("\\+")) {

                        batches.add(
                                Integer.parseInt(s)
                        );
                    }

                    batchTracker.put(
                            path,
                            batches
                    );
                }

                br.close();
            }

        } catch (Exception e) {

            e.printStackTrace();

            throw new RuntimeException(e);
        }

        // =====================================
        // SORT DESCENDING
        // =====================================

        List<Map<String, Object>> output =
                new ArrayList<>(
                        finalMap.values()
                );

        output.sort(
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

        // =====================================
        // TOP 20
        // =====================================

        if (output.size() > 20) {

            output =
                    new ArrayList<>(
                            output.subList(0, 20)
                    );
        }

        // =====================================
        // SORT ASCENDING FOR DISPLAY
        // =====================================

        output.sort(
                Comparator.comparing(
                                (Map<String, Object> d) ->
                                        (Integer) d.get("request_count")
                        )
                        .thenComparing(
                                d ->
                                        (String) d.get("resource_path")
                        )
        );

        // =====================================
        // BUILD FINAL ROWS
        // =====================================

        List<Map<String, Object>> rows =
                new ArrayList<>();

        for (Map<String, Object> doc : output) {

            String path =
                    (String) doc.get("resource_path");

            Set<Integer> batches =
                    batchTracker.get(path);

            List<Integer> sortedBatches =
                    new ArrayList<>(batches);

            Collections.sort(sortedBatches);

            String batchString =
                    String.join(
                            "+",
                            sortedBatches.stream()
                                    .map(String::valueOf)
                                    .toArray(String[]::new)
                    );

            Map<String, Object> row =
                    new LinkedHashMap<>();

            row.put(
                    "resource_path",
                    path
            );

            row.put(
                    "request_count",
                    doc.get("request_count")
            );

            row.put(
                    "total_bytes",
                    doc.get("total_bytes")
            );

            row.put(
                    "distinct_hosts",
                    doc.get("distinct_hosts")
            );

            row.put(
                    "batch_id",
                    batchString
            );

            rows.add(row);
        }

        // =====================================
        // DEBUG
        // =====================================

        System.out.println(
                "Rows to insert: "
                        + rows.size()
        );

        // =====================================
        // INSERT INTO POSTGRES
        // =====================================

        PostgresInsertService.Insert(
                "mapreduce",
                "query_2",
                rows
        );

        // =====================================
        // PRINT OUTPUT
        // =====================================

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
    }

    // =====================================
    // MAPPER
    // =====================================

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

                    host	rawTimestamp	date	hour	method	path	protocol	status	bytes
                 */

                String[] parts =
                        line.split("\\t");

                if (parts.length < 9) {

                    System.out.println(
                            "Skipping malformed line: "
                                    + line
                    );

                    return;
                }

                // =====================================
                // GET BATCH ID
                // =====================================

                FileSplit split =
                        (FileSplit)
                                context.getInputSplit();

                String fileName =
                        split.getPath().getName();

                String numeric =
                        fileName.replaceAll(
                                "[^0-9]",
                                ""
                        );

                int batchId =
                        Integer.parseInt(numeric) + 1;

                String host =
                        parts[0];

                String path =
                        parts[5];

                long bytes = 0;

                if (!parts[8].equals("-")) {

                    bytes =
                            Long.parseLong(parts[8]);
                }

                /*
                    KEY:
                        path

                    VALUE:
                        count,bytes,host,batch
                 */

                String mapValue =
                        "1,"
                                + bytes
                                + ","
                                + host
                                + ","
                                + batchId;

                context.write(
                        new Text(path),
                        new Text(mapValue)
                );

            } catch (Exception e) {

                System.out.println(
                        "Mapper Error: "
                                + e.getMessage()
                );
            }
        }
    }

    // =====================================
    // REDUCER
    // =====================================

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

                int totalCount = 0;

                long totalBytes = 0;

                Set<String> hosts =
                        new HashSet<>();

                Set<Integer> batches =
                        new HashSet<>();

                for (Text value : values) {

                    String[] parts =
                            value.toString().split(",");

                    totalCount +=
                            Integer.parseInt(parts[0]);

                    totalBytes +=
                            Long.parseLong(parts[1]);

                    hosts.add(parts[2]);

                    batches.add(
                            Integer.parseInt(parts[3])
                    );
                }

                // =====================================
                // SORT BATCH IDS
                // =====================================

                List<Integer> sorted =
                        new ArrayList<>(batches);

                Collections.sort(sorted);

                String batchString =
                        String.join(
                                "+",
                                sorted.stream()
                                        .map(String::valueOf)
                                        .toArray(String[]::new)
                        );

                /*
                    OUTPUT FORMAT:

                    path,count,totalBytes,distinctHosts,batches
                 */

                String output =
                        key.toString()
                                + "\t"
                                + totalCount
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