package com.example.mapReduce.service;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public final class HadoopClusterManager {

    private HadoopClusterManager() {
    }

    public static void restartHadoopCluster() {

        try {

            System.out.println("\nStopping YARN...");
            runCommand("stop-yarn.sh");

            System.out.println("\nStopping HDFS...");
            runCommand("stop-dfs.sh");

            Thread.sleep(3000);

            System.out.println("\nStarting HDFS...");
            runCommand("start-dfs.sh");

            Thread.sleep(5000);

            System.out.println("\nStarting YARN...");
            runCommand("start-yarn.sh");

            Thread.sleep(5000);

            System.out.println("\nLeaving Safe Mode...");
            runCommand("hdfs dfsadmin -safemode leave");

            System.out.println("\nHadoop cluster restarted successfully.");

        } catch (Exception e) {

            throw new RuntimeException(e);
        }
    }

    private static void runCommand(String command)
            throws Exception {

        ProcessBuilder builder =
                new ProcessBuilder(
                        "bash",
                        "-c",
                        command
                );

        builder.redirectErrorStream(true);

        Process process =
                builder.start();

        BufferedReader reader =
                new BufferedReader(
                        new InputStreamReader(
                                process.getInputStream()
                        )
                );

        String line;

        while ((line = reader.readLine()) != null) {

            System.out.println(line);
        }

        int exitCode =
                process.waitFor();

        if (exitCode != 0) {

            throw new RuntimeException(
                    "Command failed: " + command
            );
        }
    }
}