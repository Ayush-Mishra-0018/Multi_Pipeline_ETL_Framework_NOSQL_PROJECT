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

public class Query3_HourlyErrorAnalysis {

    public static void run() {

        System.out.println(
                "\n\n Hourly Error Analysis \n\n"
        );

        // Key = date_hour
        Map<String, Map<String, Object>> finalMap =
                new HashMap<>();


        Map<String, Set<Integer>> batchTracker =
                new HashMap<>();

        try {

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


            conf.set(

                    "mapreduce.job.jar",

                    ConfigReader.get("mapreduce.job.jar")

            );

            Job job =

                    Job.getInstance(
                            conf,
                            "Query3 Hourly Error Analysis"

                    );

            job.setJarByClass(
                    Query3_HourlyErrorAnalysis.class
            );

            job.setMapperClass(
                    Query3Mapper.class
            );

            job.setReducerClass(
                    Query3Reducer.class
            );


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



            String inputPath =
                    ConfigReader.get(
                            "hdfs.output.filtered.path"
                    );

            String outputPath =
                    ConfigReader.get(
                            "hdfs.output.path"
                    ) + "/query_3";

            Path outputDir =
                    new Path(outputPath);

            FileSystem fs =
                    FileSystem.get(conf);


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


            boolean success =
                    job.waitForCompletion(true);

            if (!success) {

                throw new RuntimeException(
                        "Query3 MapReduce job failed"
                );
            }


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


                    String[] parts =
                            line.split(",");

                    if (parts.length < 6) {
                        continue;
                    }

                    String date =
                            parts[0];

                    int hour =
                            Integer.parseInt(parts[1]);

                    int errorCount =
                            Integer.parseInt(parts[2]);

                    int totalCount =
                            Integer.parseInt(parts[3]);

                    int distinctHosts =
                            Integer.parseInt(parts[4]);

                    String batchString =
                            parts[5];

                    String key =
                            date + "_" + hour;

                    Map<String, Object> rowData =
                            new HashMap<>();

                    rowData.put(
                            "log_date",
                            date
                    );

                    rowData.put(
                            "log_hour",
                            hour
                    );

                    rowData.put(
                            "error_request_count",
                            errorCount
                    );

                    rowData.put(
                            "total_request_count",
                            totalCount
                    );

                    rowData.put(
                            "distinct_error_hosts",
                            distinctHosts
                    );

                    finalMap.put(
                            key,
                            rowData
                    );


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
                                (Integer) d.get("log_hour")
                )
        );

        List<Map<String, Object>> rows =
                new ArrayList<>();

        for (Map<String, Object> doc : output) {

            int errors =
                    (Integer) doc.get("error_request_count");

            int total =
                    (Integer) doc.get("total_request_count");

            double errorRate =
                    total == 0 ? 0 :
                            Math.round(
                                    (errors * 10000.0 / total)
                            ) / 100.0;

            String key =
                    doc.get("log_date")
                            + "_"
                            + doc.get("log_hour");

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


            Map<String, Object> row =
                    new LinkedHashMap<>();

            row.put(
                    "log_date",
                    formattedDate
            );

            row.put(
                    "log_hour",
                    doc.get("log_hour")
            );

            row.put(
                    "error_request_count",
                    errors
            );

            row.put(
                    "total_request_count",
                    total
            );

            row.put(
                    "error_rate",
                    errorRate
            );

            row.put(
                    "distinct_error_hosts",
                    doc.get("distinct_error_hosts")
            );

            row.put(
                    "batch_id",
                    batchString
            );

            rows.add(row);
        }

        System.out.println(
                "Rows to insert: "
                        + rows.size()
        );

        PostgresInsertService.Insert(
                "mapreduce",
                "query_3",
                rows
        );


        System.out.printf(
                "%-12s | %-10s | %-20s | %-20s | %-12s | %-20s | %-10s%n",
                "log_date",
                "hour",
                "error_requests",
                "total_requests",
                "error_rate",
                "distinct_hosts",
                "batches"
        );

        System.out.println(
                "----------------------------------------------------------------------------------------------------------------------------------------------------------------------------"
        );

        for (Map<String, Object> row : rows) {

            System.out.printf(
                    "%-12s | %-10d | %-20d | %-20d | %-12.2f | %-20d | %-10s%n",
                    row.get("log_date"),
                    row.get("log_hour"),
                    row.get("error_request_count"),
                    row.get("total_request_count"),
                    row.get("error_rate"),
                    row.get("distinct_error_hosts"),
                    row.get("batch_id")
            );
        }
    }

    public static class Query3Mapper
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

                String[] parts =
                        line.split("\\t");

                if (parts.length < 9) {

                    System.out.println(
                            "Skipping malformed line: "
                                    + line
                    );

                    return;
                }

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

                String date =
                        parts[2];

                int hour =
                        Integer.parseInt(parts[3]);

                int status =
                        Integer.parseInt(parts[7]);


                int isError =
                        (status >= 400 && status <= 599)
                                ? 1
                                : 0;


                String errorHost =
                        isError == 1
                                ? host
                                : "NO_ERROR";

                String mapValue =
                        isError
                                + ",1,"
                                + errorHost
                                + ","
                                + batchId;

                String mapKey =
                        date + "_" + hour;

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

    public static class Query3Reducer
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

                int errorCount = 0;

                int totalCount = 0;

                Set<String> hosts =
                        new HashSet<>();

                Set<Integer> batches =
                        new HashSet<>();

                for (Text value : values) {

                    String[] parts =
                            value.toString().split(",");

                    errorCount +=
                            Integer.parseInt(parts[0]);

                    totalCount +=
                            Integer.parseInt(parts[1]);

                    if (!parts[2].equals("NO_ERROR")) {

                        hosts.add(parts[2]);
                    }

                    batches.add(
                            Integer.parseInt(parts[3])
                    );
                }

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

                String output =
                        keyParts[0]
                                + ","
                                + keyParts[1]
                                + ","
                                + errorCount
                                + ","
                                + totalCount
                                + ","
                                + hosts.size()
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