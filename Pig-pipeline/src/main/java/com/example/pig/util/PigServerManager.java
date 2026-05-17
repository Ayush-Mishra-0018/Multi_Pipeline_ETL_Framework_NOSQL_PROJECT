package com.example.pig.util;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;

import java.io.IOException;
import java.util.Properties;

/**
 * Manages a thread-local {@link PigServer} instance running in
 * {@link ExecType#LOCAL} mode — the embedded API equivalent of "pig -x local".
 *
 * <h3>Why LOCAL instead of MAPREDUCE?</h3>
 * {@code ExecType.MAPREDUCE} requires {@code core-site.xml} or
 * {@code hadoop-site.xml} on the classpath so Pig can locate a cluster.
 * Without a real Hadoop installation those files don't exist, causing
 * ERROR 4010. {@code ExecType.LOCAL} needs no config files: it runs the
 * same Pig Latin scripts entirely inside this JVM using a local file-system
 * execution engine.
 *
 * <h3>Why this is still faster than the original ProcessBuilder approach</h3>
 * The original code spawned a new {@code pig} process per batch, paying
 * 10–30 s of JVM + Pig bootstrap overhead every time. With the embedded API,
 * {@link PigServer} is created once per thread (thread-local) and reused for
 * every batch that thread handles — startup cost is paid exactly once.
 *
 * <h3>Why thread-local?</h3>
 * The query runners submit one {@code Callable} per batch to an
 * {@link java.util.concurrent.ExecutorService}. {@link PigServer} is not
 * thread-safe, so each worker thread needs its own instance.
 */
public final class PigServerManager {

    private static final ThreadLocal<PigServer> THREAD_LOCAL =
            ThreadLocal.withInitial(PigServerManager::createPigServer);

    private PigServerManager() {}

    /** Returns (or lazily creates) the {@link PigServer} for the calling thread. */
    public static PigServer get() {
        return THREAD_LOCAL.get();
    }

    /**
     * Shuts down and removes the {@link PigServer} for the calling thread.
     * Call this in a {@code finally} block at the end of each worker thread.
     */
    public static void close() {
        PigServer ps = THREAD_LOCAL.get();
        if (ps != null) {
            ps.shutdown();
        }
        THREAD_LOCAL.remove();
    }

    // -------------------------------------------------------------------------

    private static PigServer createPigServer() {
        try {
            Properties props = new Properties();

            // Suppress Pig's verbose job-progress output (same noise the original
            // code already silenced with the commented-out System.out.println).
            props.setProperty("pig.logfile", "/dev/null");

            // Partial aggregation in the map phase — reduces shuffle volume.
            props.setProperty("pig.exec.mapPartAgg", "true");

            // ExecType.LOCAL = "pig -x local":
            //   - No hadoop-site.xml / core-site.xml required
            //   - Runs entirely inside this JVM
            //   - Uses the local filesystem directly
            return new PigServer(ExecType.LOCAL, props);

        } catch (IOException e) {
            throw new RuntimeException("Failed to initialise PigServer", e);
        }
    }
}