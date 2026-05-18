package com.example.mapReduce.jobs.batchProcessing;

import com.example.config.ConfigReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.PrintWriter;

public final class BatchProcessingDriver {

    private BatchProcessingDriver() {
    }

    public static boolean runJob() {

        // TRACK TOTAL TIME FROM HERE
        long startTime = System.currentTimeMillis();

        try {

            // =====================================
            // HADOOP CONFIGURATION
            // =====================================

            Configuration conf = new Configuration();


            conf.addResource(new Path(ConfigReader.get("hadoop.core.site")));
            conf.addResource(new Path(ConfigReader.get("hadoop.hdfs.site")));
            conf.addResource(new Path(ConfigReader.get("hadoop.mapred.site")));
            conf.addResource(new Path(ConfigReader.get("hadoop.yarn.site")));

            conf.set("fs.defaultFS",             ConfigReader.get("hdfs.uri"));
            conf.set("mapreduce.framework.name", ConfigReader.get("yarn.framework.name"));

            conf.set("yarn.resourcemanager.address",
                    ConfigReader.get("yarn.resourcemanager.address"));
            conf.set("yarn.resourcemanager.scheduler.address",
                    ConfigReader.get("yarn.resourcemanager.scheduler.address"));
            conf.set("yarn.resourcemanager.resource-tracker.address",
                    ConfigReader.get("yarn.resourcemanager.resource-tracker.address"));
            conf.set(
                    "mapreduce.job.jar",
                    ConfigReader.get("mapreduce.job.jar")
            );

            conf.set("fs.hdfs.impl",
                    org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            conf.set("fs.file.impl",
                    org.apache.hadoop.fs.LocalFileSystem.class.getName());

            // =====================================
            // CREATE JOB
            // =====================================

            Job job = Job.getInstance(conf, "Batch Processing Job");
            job.setJarByClass(BatchProcessingDriver.class);

            // =====================================
            // INPUT FORMAT
            // =====================================

            job.setInputFormatClass(NLineInputFormat.class);

            int batchSize = Integer.parseInt(ConfigReader.get("batch.size"));
            NLineInputFormat.setNumLinesPerSplit(job, batchSize);

            // =====================================
            // MAPPER + NO REDUCER
            // =====================================

            job.setMapperClass(BatchProcessingMapper.class);
            job.setNumReduceTasks(0);

            // =====================================
            // OUTPUT TYPES
            // =====================================
            job.setMapOutputKeyClass(NullWritable.class);
            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);

            // =====================================
            // MULTIPLE OUTPUTS
            // filtered/ and malformed/ are written
            // by MultipleOutputs inside the mapper
            // =====================================

            MultipleOutputs.addNamedOutput(
                    job,
                    "filtered",
                    TextOutputFormat.class,
                    NullWritable.class,
                    Text.class
            );

            MultipleOutputs.addNamedOutput(
                    job,
                    "malformed",
                    TextOutputFormat.class,
                    NullWritable.class,
                    Text.class
            );

            // =====================================
            // INPUT PATH
            // =====================================

            String inputPath  = ConfigReader.get("hdfs.input.path");

            // JOB NEEDS A BASE OUTPUT DIR EVEN WITH MULTIPLE OUTPUTS
            String baseOutput = ConfigReader.get("hdfs.output.path") + "/batch_processing";
            Path   outputDir  = new Path(baseOutput);

            FileSystem fs = FileSystem.get(conf);

            // =====================================
            // CLEAN OLD OUTPUT DIRS
            // =====================================

            cleanDir(fs, baseOutput);
            cleanDir(fs, ConfigReader.get("hdfs.output.filtered.path"));
            cleanDir(fs, ConfigReader.get("hdfs.output.malformed.path"));
            cleanDir(fs, ConfigReader.get("hdfs.output.stats.path"));

            FileInputFormat.addInputPath(job, new Path(inputPath));
            FileOutputFormat.setOutputPath(job, outputDir);

            // =====================================
            // RUN JOB
            // =====================================

            boolean success = job.waitForCompletion(true);

            System.out.println("JOB SUCCESS = " + success);

            if (!success) {

                System.out.println(
                        "FAILURE INFO: " +
                                job.getStatus().getFailureInfo()
                );

                throw new RuntimeException("Job failed");
            }

            // =====================================
            // WRITE STATS
            // =====================================

            if (success) {

                long endTime   = System.currentTimeMillis();
                long totalTime = endTime - startTime;

                Counters counters = job.getCounters();

                long totalRecords  = counters
                        .findCounter(BatchProcessingMapper.LogCounters.TOTAL_RECORDS)
                        .getValue();

                long totalValid    = counters
                        .findCounter(BatchProcessingMapper.LogCounters.TOTAL_VALID)
                        .getValue();

                long totalMalformed = counters
                        .findCounter(BatchProcessingMapper.LogCounters.TOTAL_MALFORMED)
                        .getValue();

                long totalBatches = counters
                        .findCounter(
                                BatchProcessingMapper.LogCounters.TOTAL_BATCHES
                        )
                        .getValue();


                double avgBatchSize = totalBatches > 0
                        ? (double) totalRecords / totalBatches
                        : 0;

                writeStats(
                        fs,
                        ConfigReader.get("hdfs.output.stats.path"),
                        totalRecords,
                        totalValid,
                        totalMalformed,
                        totalBatches,
                        avgBatchSize,
                        totalTime
                );

                System.out.println("\nBatch processing completed successfully.");
                System.out.println("Total time: " + totalTime + "ms");

            } else {
                System.out.println("\nBatch processing failed.");
            }

            return success;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

//        return false;
    }

    // =====================================
    // WRITE STATS TO HDFS
    // =====================================

    private static void writeStats(
            FileSystem fs,
            String statsPath,
            long totalRecords,
            long totalValid,
            long totalMalformed,
            long totalBatches,
            double avgBatchSize,
            long totalTime
    ) throws Exception {

        Path statsFile = new Path(statsPath + "/stats.txt");
        fs.mkdirs(new Path(statsPath));

        try (FSDataOutputStream out = fs.create(statsFile, true);
             PrintWriter writer = new PrintWriter(out)) {

            writer.println("totalRecordsProcessed=" + totalRecords);
            writer.println("totalValid="            + totalValid);
            writer.println("totalMalformed="        + totalMalformed);
            writer.println("totalBatches="          + totalBatches);
            writer.println("avgBatchSize="          + String.format("%.2f", avgBatchSize));
            writer.println("totalTime="             + totalTime + "ms");
        }

        System.out.println("Stats written to: " + statsFile);
    }

    // =====================================
    // CLEAN DIRECTORY HELPER
    // =====================================

    private static void cleanDir(FileSystem fs, String path) throws Exception {
        Path p = new Path(path);
        if (fs.exists(p)) {
            fs.delete(p, true);
            System.out.println("Deleted: " + path);
        }
    }


}