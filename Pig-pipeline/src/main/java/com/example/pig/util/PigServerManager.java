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
 * <h3>Thread-safety note</h3>
 * {@link PigServer} in LOCAL mode is <em>not safe to use concurrently</em>
 * across multiple threads in the same JVM — Hadoop's {@code Configuration}
 * object has shared static state that causes non-deterministic failures when
 * more than one Pig job runs at the same time.  The query runners therefore
 * use a single-threaded executor so that all batches for a given query run
 * sequentially on one thread.  This {@link ThreadLocal} wrapper ensures the
 * single worker thread creates its {@link PigServer} once and reuses it for
 * every batch, avoiding the per-batch JVM-startup cost that the old
 * {@code ProcessBuilder} approach paid.
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
     * Call this <em>once</em>, after all batches for a query have finished,
     * not after every individual batch.
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

            // Suppress Pig's verbose job-progress output.
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