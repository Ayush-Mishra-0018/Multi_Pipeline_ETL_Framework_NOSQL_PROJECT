package com.example.pig.util;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;

import java.io.IOException;
import java.util.Properties;


public final class PigServerManager {

    private static final ThreadLocal<PigServer> THREAD_LOCAL =
            ThreadLocal.withInitial(PigServerManager::createPigServer);

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
    }


    private static PigServer createPigServer() {
        try {
            Properties props = new Properties();

            // Suppress Pig's verbose job-progress output.
            props.setProperty("pig.logfile", "/dev/null");

            props.setProperty("pig.exec.mapPartAgg", "true");

            return new PigServer(ExecType.LOCAL, props);

        } catch (IOException e) {
            throw new RuntimeException("Failed to initialise PigServer", e);
        }
    }
}