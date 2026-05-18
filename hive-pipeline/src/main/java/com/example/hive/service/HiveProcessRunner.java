package com.example.hive.service;

import com.example.config.ConfigReader;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * HiveProcessRunner
 *
 * Invokes the Hive CLI via ProcessBuilder using:
 *     hive [--hiveconf k=v ...] -f /path/to/script.hql
 *     hive [--hiveconf k=v ...] -e "SELECT ..."
 *
 * IMPORTANT:
 * - NO HiveServer2
 * - NO Beeline
 * - NO JDBC
 * - Hive CLI only
 *
 * ALL ETL logic happens inside HiveQL.
 * Java only orchestrates execution and captures final outputs.
 */
public final class HiveProcessRunner {

    private static final String HIVE_HOME =
            ConfigReader.get("hive.home", "/home/ayush/hive");

    private static final String HIVE_BIN =
            HIVE_HOME + "/bin/hive";

    private static final String HADOOP_HOME =
            ConfigReader.get("hadoop.home", "/home/ayush/hadoop");

    /**
     * Hive 3.x requires Java 8.
     * Entire project still runs on Java 11.
     * Only Hive subprocess uses Java 8.
     */
    private static final String JAVA8_HOME =
            ConfigReader.get(
                    "hive.java8.home",
                    "/usr/lib/jvm/java-8-openjdk-amd64"
            );

    /**
     * IMPORTANT:
     * Derby metastore uses relative path:
     *     metastore_db
     *
     * So subprocess working directory must match
     * the same directory where manual `hive`
     * execution works correctly.
     */
    private static final File HIVE_WORK_DIR =
            new File(System.getProperty("user.home"));

    private HiveProcessRunner() {
    }

    // ============================================================
    // Execute HQL script file
    // ============================================================

    public static List<String> runScript(
            String scriptPath,
            String... hiveConfs
    ) throws Exception {

        ProcessBuilder pb = new ProcessBuilder(
                buildArgs(hiveConfs, "-f", scriptPath)
        );

        configure(pb);

        return execute(pb, "script:" + scriptPath);
    }


    public static List<String> runInline(
            String hql,
            String... hiveConfs
    ) throws Exception {

        ProcessBuilder pb = new ProcessBuilder(
                buildArgs(hiveConfs, "-e", hql)
        );

        configure(pb);

        return execute(
                pb,
                "inline:" + hql.substring(0, Math.min(hql.length(), 60))
        );
    }

    private static List<String> buildArgs(
            String[] hiveConfs,
            String flag,
            String value
    ) {

        List<String> args = new ArrayList<>();

        args.add(HIVE_BIN);

        // Suppress column headers
        args.add("--hiveconf");
        args.add("hive.cli.print.header=false");

        // Additional hiveconf pairs
        for (String kv : hiveConfs) {
            args.add("--hiveconf");
            args.add(kv);
        }

        args.add(flag);
        args.add(value);

        return args;
    }

    private static void configure(ProcessBuilder pb) {

        Map<String, String> env = pb.environment();

        env.put("JAVA_HOME", JAVA8_HOME);

        String java8Bin = JAVA8_HOME + "/bin";

        String currentPath =
                env.getOrDefault("PATH", "");

        env.put(
                "PATH",
                java8Bin + ":" + currentPath
        );


        env.put("HADOOP_HOME", HADOOP_HOME);

        env.put("HIVE_HOME", HIVE_HOME);

        env.put(
                "HIVE_CONF_DIR",
                HIVE_HOME + "/conf"
        );



        env.put(
                "HADOOP_CLASSPATH",
                HADOOP_HOME + "/share/hadoop/common/*:" +
                        HADOOP_HOME + "/share/hadoop/common/lib/*:" +
                        HADOOP_HOME + "/share/hadoop/hdfs/*:" +
                        HADOOP_HOME + "/share/hadoop/hdfs/lib/*:" +
                        HADOOP_HOME + "/share/hadoop/mapreduce/*:" +
                        HADOOP_HOME + "/share/hadoop/yarn/*"
        );


        env.put(
                "CLASSPATH",
                HIVE_HOME + "/lib/*"
        );


        pb.directory(HIVE_WORK_DIR);

        pb.redirectErrorStream(false);
    }

    private static void debugEnv(Map<String, String> env) {

        System.out.println(
                "\n[HiveProcessRunner] ===== Subprocess Env Debug ====="
        );

        String[] keys = {
                "JAVA_HOME",
                "PATH",
                "HADOOP_HOME",
                "HIVE_HOME",
                "HIVE_CONF_DIR",
                "CLASSPATH",
                "HADOOP_CLASSPATH",
                "JAVA_TOOL_OPTIONS",
                "USER",
                "HOME"
        };

        for (String k : keys) {

            String v = env.getOrDefault(k, "<not set>");

            if ("PATH".equals(k) && v.length() > 120) {
                v = v.substring(0, 120) + "...";
            }

            System.out.println(
                    "[HiveProcessRunner]   " + k + "=" + v
            );
        }

        runDebugProbe(env, "which java");
        runDebugProbe(env, "java -version");

        System.out.println(
                "[HiveProcessRunner] ==================================\n"
        );
    }


    private static void runDebugProbe(
            Map<String, String> env,
            String command
    ) {

        try {

            ProcessBuilder pb =
                    new ProcessBuilder(
                            "bash",
                            "-c",
                            command + " 2>&1"
                    );

            pb.environment().putAll(env);

            pb.directory(HIVE_WORK_DIR);

            pb.redirectErrorStream(true);

            Process proc = pb.start();

            try (BufferedReader r = new BufferedReader(
                    new InputStreamReader(proc.getInputStream()))) {

                String line;

                while ((line = r.readLine()) != null) {

                    System.out.println(
                            "[HiveProcessRunner]   [probe:" +
                                    command + "] " + line
                    );
                }
            }

            proc.waitFor();

        } catch (Exception e) {

            System.err.println(
                    "[HiveProcessRunner] probe failed: " +
                            e.getMessage()
            );
        }
    }

    // ============================================================
    // Execute process
    // ============================================================

    private static List<String> execute(
            ProcessBuilder pb,
            String label
    ) throws Exception {

        System.out.println(
                "[HiveProcessRunner] executing -> " + label
        );

        System.out.println(
                "[HiveProcessRunner] working dir -> " +
                        HIVE_WORK_DIR.getAbsolutePath()
        );

        debugEnv(pb.environment());

        Process proc = pb.start();

        List<String> output = new ArrayList<>();

        // stdout
        Thread outThread = new Thread(() -> {

            try (BufferedReader r = new BufferedReader(
                    new InputStreamReader(proc.getInputStream()))) {

                String line;

                while ((line = r.readLine()) != null) {

                    if (!line.isBlank()) {
                        output.add(line);
                    }
                }

            } catch (Exception e) {

                System.err.println(
                        "[HiveProcessRunner] stdout error: " +
                                e.getMessage()
                );
            }

        }, "hive-stdout");

        // stderr
        Thread errThread = new Thread(() -> {

            try (BufferedReader r = new BufferedReader(
                    new InputStreamReader(proc.getErrorStream()))) {

                String line;

                while ((line = r.readLine()) != null) {

                    System.err.println(
                            "[HIVE-STDERR] " + line
                    );
                }

            } catch (Exception e) {

                System.err.println(
                        "[HiveProcessRunner] stderr error: " +
                                e.getMessage()
                );
            }

        }, "hive-stderr");

        outThread.start();
        errThread.start();

        int exit = proc.waitFor();
        proc.destroy();

        if (proc.isAlive()) {
            proc.destroyForcibly();
        }

        outThread.join();
        errThread.join();

        if (exit != 0) {

            throw new RuntimeException(
                    "[HiveProcessRunner] Hive exited with code " +
                            exit +
                            " for: " +
                            label
            );
        }

        System.out.println(
                "[HiveProcessRunner] done (" +
                        label +
                        ") -> " +
                        output.size() +
                        " lines"
        );

        return output;
    }
}