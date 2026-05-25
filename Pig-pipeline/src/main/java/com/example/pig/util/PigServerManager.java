package com.example.pig.util;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;

import java.io.File;
import java.io.IOException;
import java.util.Properties;


public final class PigServerManager {

    private static final ThreadLocal<PigServer> THREAD_LOCAL =
            ThreadLocal.withInitial(PigServerManager::createPigServer);

    private static final ThreadLocal<String> TEMP_DIR_HOLDER = new ThreadLocal<>();

    private PigServerManager() {}

    public static PigServer get() {
        return THREAD_LOCAL.get();
    }


    public static void close() {
        PigServer ps = THREAD_LOCAL.get();
        if (ps != null) {
            ps.shutdown();
        }
        THREAD_LOCAL.remove();

        String tempDir = TEMP_DIR_HOLDER.get();
        if (tempDir != null) {
            deleteDirectory(new File(tempDir));
            TEMP_DIR_HOLDER.remove();
        }
    }


    private static PigServer createPigServer() {
        try {
            Properties props = new Properties();

            // Suppress Pig's verbose job-progress output.
            props.setProperty("pig.logfile", "/dev/null");
            props.setProperty("pig.exec.mapPartAgg", "true");

            // Disable SchemaTuple code generation to prevent concurrent NPEs
            props.setProperty("pig.schematuple", "false");

            // Isolate temporary and staging directories for concurrent execution
            String threadUniqueId = Thread.currentThread().getName() + "_" + Thread.currentThread().getId() + "_" + System.currentTimeMillis();
            String tempDir = "./pig_temp/" + threadUniqueId;
            TEMP_DIR_HOLDER.set(tempDir);

            props.setProperty("hadoop.tmp.dir", tempDir);
            props.setProperty("mapreduce.jobtracker.system.dir", tempDir + "/system");
            props.setProperty("mapreduce.jobtracker.staging.root.dir", tempDir + "/staging");
            props.setProperty("mapreduce.cluster.temp.dir", tempDir + "/temp");
            props.setProperty("mapred.child.tmp", tempDir + "/child_tmp");

            return new PigServer(ExecType.LOCAL, props);

        } catch (IOException e) {
            throw new RuntimeException("Failed to initialise PigServer", e);
        }
    }

    private static void deleteDirectory(File file) {
        if (file.isDirectory()) {
            File[] children = file.listFiles();
            if (children != null) {
                for (File child : children) {
                    deleteDirectory(child);
                }
            }
        }
        file.delete();
    }
}