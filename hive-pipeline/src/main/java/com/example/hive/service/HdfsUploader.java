package com.example.hive.service;

import com.example.config.ConfigReader;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * HdfsUploader
 *
 * Java responsibility: upload local log files to HDFS so that
 * Hive's managed table can LOAD DATA INPATH them.
 *
 * This class does NOT read, parse, or process any log content.
 * It only moves files to HDFS using ProcessBuilder + hdfs dfs commands.
 *
 * JAVA_HOME is pinned to Java 8 for the hdfs subprocess as well
 * (for environment consistency; hdfs itself is Java-version-agnostic).
 */
public final class HdfsUploader {

    private static final String HADOOP_HOME =
            ConfigReader.get("hadoop.home", "/home/ayush/hadoop");

    private static final String HDFS_CMD =
            HADOOP_HOME + "/bin/hdfs";

    private static final String HDFS_URI =
            ConfigReader.get("hdfs.uri", "hdfs://localhost:9000");

    /**
     * Java 8 home — pinned for subprocess environment consistency.
     * Keeps the hdfs child process on the same JVM as Hive.
     */
    private static final String JAVA8_HOME =
            ConfigReader.get("hive.java8.home", "/usr/lib/jvm/java-8-openjdk-amd64");

    private HdfsUploader() {
    }

    /**
     * Upload a local file to an HDFS directory.
     * Creates the HDFS target directory if it does not exist (idempotent).
     * Clears any existing content in the directory before uploading.
     *
     * @param localFilePath  Absolute or relative path to the local file
     * @param hdfsTargetDir  HDFS directory path (without the URI prefix),
     *                       e.g. "/nasa_raw/batch_1"
     */
    public static void upload(
            String localFilePath,
            String hdfsTargetDir
    ) throws Exception {

        // Normalize relative paths into absolute canonical paths
        // so subprocess working-directory differences do not break uploads.
        String normalizedLocalPath =
                new File(localFilePath).getCanonicalPath();

        String fullHdfsDir = HDFS_URI + hdfsTargetDir;

        // Step 1: Remove the entire HDFS directory so re-runs are clean.
        // Wildcard expansion (fullHdfsDir + "/*") is NOT handled by the
        // HDFS CLI — the shell glob never runs inside ProcessBuilder, so
        // the literal string ".../*" is passed to HDFS and it fails.
        // Correct approach: delete the whole directory, then recreate it.
        // Failure here is acceptable (directory may not exist yet).
        try {
            runHdfsCommand(
                    "dfs", "-rm", "-r", "-skipTrash",
                    fullHdfsDir
            );
        } catch (Exception ignored) {
            // Directory did not exist yet — nothing to remove.
        }

        // Step 2: (Re)create the HDFS directory fresh.
        runHdfsCommand("dfs", "-mkdir", "-p", fullHdfsDir);

        // Step 3: Upload local file to HDFS directory
        runHdfsCommand(
                "dfs", "-put", "-f",
                normalizedLocalPath,
                fullHdfsDir + "/"
        );

        System.out.println(
                "[HdfsUploader] Uploaded: " + normalizedLocalPath +
                        " → " + fullHdfsDir
        );
    }

    // --- private helpers ----------------------------------------------------

    private static void runHdfsCommand(String... args) throws Exception {

        List<String> command = new ArrayList<>();
        command.add(HDFS_CMD);

        for (String arg : args) {
            command.add(arg);
        }

        System.out.println("[HdfsUploader] " + String.join(" ", command));

        ProcessBuilder pb = new ProcessBuilder(command);

        // Environment policy mirrors HiveProcessRunner exactly:
        //   - Inherit full parent env (DO NOT strip CLASSPATH/HADOOP_CLASSPATH)
        //   - Strip only JAVA_TOOL_OPTIONS (Maven JVM agent flags)
        //   - Pin JAVA_HOME=Java8, prepend Java8 bin to PATH
        //   - Pin HADOOP_HOME, HIVE_HOME
        //   - Set CWD to $HOME (same as HiveProcessRunner)
        Map<String, String> env = pb.environment();

        env.putAll(System.getenv());

        env.remove("JAVA_TOOL_OPTIONS");

        env.put("JAVA_HOME", JAVA8_HOME);
        env.put("HADOOP_HOME", HADOOP_HOME);

        String java8Bin = JAVA8_HOME + "/bin";
        String currentPath = env.getOrDefault("PATH", "");

        env.put("PATH", java8Bin + ":" + currentPath);

        pb.directory(new File(System.getProperty("user.home")));

        pb.redirectErrorStream(false);

        Process process = pb.start();

        // Drain stderr so the process doesn't block on a full stderr buffer
        Thread errDrain = new Thread(() -> {
            try (var reader = new java.io.BufferedReader(
                    new java.io.InputStreamReader(process.getErrorStream()))) {

                String line;

                while ((line = reader.readLine()) != null) {
                    System.err.println("[HDFS-STDERR] " + line);
                }

            } catch (Exception ignored) {
            }
        }, "hdfs-stderr");

        errDrain.start();

        int exitCode = process.waitFor();

        errDrain.join();

        // Non-zero exit is acceptable for the "rm /*" on an empty directory.
        // Only throw on a failed -put (which is the real upload step).
        if (exitCode != 0 && args.length >= 2 && "-put".equals(args[1])) {

            throw new RuntimeException(
                    "[HdfsUploader] hdfs dfs -put failed with exit code: " +
                            exitCode
            );
        }
    }
}