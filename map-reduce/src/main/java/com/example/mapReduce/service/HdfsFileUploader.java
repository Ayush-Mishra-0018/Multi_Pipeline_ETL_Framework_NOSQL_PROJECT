package com.example.mapReduce.service;

import com.example.config.ConfigReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

public final class HdfsFileUploader {

    private static final String INPUT_PATH =
            ConfigReader.get("hdfs.input.path");

    private static final String FILE_PATHS =
            ConfigReader.get("input.file.paths");

    private HdfsFileUploader() {
    }

    public static void uploadInputDataIfMissing() {

        try {

            Configuration conf =
                    new Configuration();

            String hdfsUri =
                    ConfigReader.get("hdfs.uri");

            FileSystem fs =
                    FileSystem.get(
                            new URI(hdfsUri),
                            conf
                    );

            Path inputDir =
                    new Path(INPUT_PATH);

            // =====================================
            // CREATE INPUT DIRECTORY IF MISSING
            // =====================================

            if (!fs.exists(inputDir)) {

                fs.mkdirs(inputDir);

                System.out.println(
                        "Created HDFS directory: " +
                                INPUT_PATH
                );
            }

            // =====================================
            // READ LOCAL FILE PATHS
            // =====================================

            String[] localFiles =
                    FILE_PATHS.split(",");

            for (String localFile : localFiles) {

                localFile =
                        localFile.trim();

                String fileName =
                        new java.io.File(localFile)
                                .getName();

                Path hdfsFilePath =
                        new Path(
                                INPUT_PATH +
                                        "/" +
                                        fileName
                        );

                // =====================================
                // CHECK IF FILE EXISTS
                // =====================================

                if (fs.exists(hdfsFilePath)) {

                    System.out.println(
                            "File already exists in HDFS: " +
                                    fileName
                    );

                    continue;
                }

                // =====================================
                // UPLOAD FILE
                // =====================================

                System.out.println(
                        "Uploading file to HDFS: " +
                                fileName
                );

                fs.copyFromLocalFile(
                        new Path(localFile),
                        hdfsFilePath
                );

                System.out.println(
                        "Uploaded: " + fileName
                );
            }

            fs.close();

            System.out.println(
                    "\nHDFS upload completed successfully."
            );

        } catch (Exception e) {

            e.printStackTrace();
        }
    }


}