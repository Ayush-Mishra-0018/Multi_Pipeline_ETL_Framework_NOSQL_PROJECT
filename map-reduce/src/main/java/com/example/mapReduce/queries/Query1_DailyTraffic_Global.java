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

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import java.util.*;

public class Query1_DailyTraffic_Global {

    public static void run() {

        System.out.println(
                "\n\n Daily Traffic Global \n\n"
        );

        /*
            FINAL AGGREGATED DATA

            key:
                date_status

            value:
                aggregated row
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

            Job job =
                    Job.getInstance(
                            conf,
                            "Query1 Daily Traffic Global"
                    );

            job.setJarByClass(
                    Query1_DailyTraffic_Global.class
            );

            // =====================================
            // MAPPER + REDUCER
            // =====================================

            job.setMapperClass(
                    Query1Mapper.class
            );

            job.setReducerClass(
                    Query1Reducer.class
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
                    ) + "/query_1";

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
                        "Query1 MapReduce job failed"
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

                        date,status,count,totalBytes,batches
                     */

                    String[] parts =
                            line.split(",");

                    if (parts.length < 5) {
                        continue;
                    }

                    String date =
                            parts[0];

                    int status =
                            Integer.parseInt(parts[1]);

                    int count =
                            Integer.parseInt(parts[2]);

                    long bytes =
                            Long.parseLong(parts[3]);

                    String batchString =
                            parts[4];

                    String key =
                            date + "_" + status;

                    Map<String, Object> rowData =
                            new HashMap<>();

                    rowData.put(
                            "log_date",
                            date
                    );

                    rowData.put(
                            "status_code",
                            status
                    );

                    rowData.put(
                            "request_count",
                            count
                    );

                    rowData.put(
                            "total_bytes",
                            bytes
                    );

                    finalMap.put(
                            key,
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
                            key,
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
        // SORT OUTPUT
        // =====================================

        List<Map<String, Object>> output =
                new ArrayList<>(
                        finalMap.values()
                );

        output.sort(
                Comparator.comparing(
                        (Map<String, Object> d) ->
                                (String) d.get("log_date")
                ).thenComparing(
                        d ->
                                (Integer) d.get("status_code")
                )
        );

        // =====================================
        // BUILD FINAL ROWS
        // =====================================

        List<Map<String, Object>> rows =
                new ArrayList<>();

        for (Map<String, Object> doc : output) {

            String key =
                    doc.get("log_date")
                            + "_"
                            + doc.get("status_code");

            Set<Integer> batches =
                    batchTracker.get(key);

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

            String formattedDate =
                    convertDate(
                            (String) doc.get("log_date")
                    );

            java.sql.Date sqlDate =
                    java.sql.Date.valueOf(
                            formattedDate
                    );

            Map<String, Object> row =
                    new LinkedHashMap<>();

            row.put(
                    "log_date",
                    sqlDate
            );

            row.put(
                    "status_code",
                    doc.get("status_code")
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
                "query_1",
                rows
        );

        // =====================================
        // PRINT OUTPUT
        // =====================================

        System.out.printf(
                "%-12s | %-12s | %-15s | %-15s | %-10s%n",
                "log_date",
                "status_code",
                "request_count",
                "total_bytes",
                "batches"
        );

        System.out.println(
                "--------------------------------------------------------------------------------------------------------------"
        );

        for (Map<String, Object> row : rows) {

            System.out.printf(
                    "%-12s | %-12d | %-15d | %-15d | %-10s%n",
                    row.get("log_date"),
                    row.get("status_code"),
                    row.get("request_count"),
                    row.get("total_bytes"),
                    row.get("batch_id")
            );
        }
    }

    // =====================================
    // MAPPER
    // =====================================

    public static class Query1Mapper
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
                // GET BATCH ID FROM FILE NAME
                // =====================================

                FileSplit split =
                        (FileSplit)
                                context.getInputSplit();

                String fileName =
                        split.getPath().getName();

                /*
                    filtered-m-00000 -> batch 1
                    filtered-m-00001 -> batch 2
                    ...
                 */

                String numeric =
                        fileName.replaceAll(
                                "[^0-9]",
                                ""
                        );

                int batchId =
                        Integer.parseInt(numeric) + 1;

                String date =
                        parts[2];

                int status =
                        Integer.parseInt(parts[7]);

                long bytes =
                        Long.parseLong(parts[8]);

                String mapKey =
                        date + "_" + status;

                String mapValue =
                        "1,"
                                + bytes
                                + ","
                                + batchId;

                context.write(
                        new Text(mapKey),
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

    public static class Query1Reducer
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

                Set<Integer> batches =
                        new HashSet<>();

                for (Text value : values) {

                    String[] parts =
                            value.toString().split(",");

                    totalCount +=
                            Integer.parseInt(parts[0]);

                    totalBytes +=
                            Long.parseLong(parts[1]);

                    batches.add(
                            Integer.parseInt(parts[2])
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

                String[] keyParts =
                        key.toString().split("_");

                /*
                    OUTPUT FORMAT:

                    date,status,count,totalBytes,batches
                 */

                String output =
                        keyParts[0]
                                + ","
                                + keyParts[1]
                                + ","
                                + totalCount
                                + ","
                                + totalBytes
                                + ","
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

    // =====================================
    // DATE CONVERTER
    // =====================================

    private static String convertDate(
            String input
    ) {

        try {

            DateTimeFormatter inputFmt =
                    DateTimeFormatter.ofPattern(
                            "dd/MMM/yyyy",
                            Locale.ENGLISH
                    );

            DateTimeFormatter outputFmt =
                    DateTimeFormatter.ofPattern(
                            "yyyy-MM-dd"
                    );

            return LocalDate.parse(
                    input,
                    inputFmt
            ).format(outputFmt);

        } catch (Exception e) {

            return input;
        }
    }
}