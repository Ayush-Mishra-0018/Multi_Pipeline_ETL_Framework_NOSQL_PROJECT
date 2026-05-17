package com.example.pig.util;

import org.apache.pig.PigServer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility that executes Pig Latin scripts through the <em>embedded Pig API</em>
 * ({@link PigServer}) instead of spawning a {@code pig} CLI subprocess.
 *
 * <h3>Why this is faster than the original {@code ProcessBuilder} approach</h3>
 * <ul>
 *   <li>No per-batch JVM startup or Pig bootstrap (~10–30 s saved per batch).
 *   <li>{@link PigServer} is reused across batches via {@link PigServerManager}
 *       (thread-local, so safe under the concurrent {@code ExecutorService}).
 *   <li>Jobs run through Hadoop's {@code LocalJobRunner}, which parallelises
 *       map tasks across all CPU cores in-process.
 * </ul>
 *
 * <h3>What has NOT changed</h3>
 * The {@code .pig} scripts themselves are identical.  Only the mechanism that
 * invokes them is different.
 */
public final class PigScriptExecutor {

    private PigScriptExecutor() {}

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    /**
     * Executes a query Pig script (query1/2/3.pig) via the embedded API.
     *
     * <p>Replaces the original {@code ProcessBuilder} call:
     * <pre>
     *   pig -x local -param INPUT_DIR=... -param OUTPUT_DIR=... -param BATCH_ID=... -f scriptPath
     * </pre>
     *
     * @param scriptPath path to the .pig script (same as before)
     * @param inputDir   value for the {@code $INPUT_DIR} Pig parameter
     * @param outputDir  value for the {@code $OUTPUT_DIR} Pig parameter
     * @param batchId    value for the {@code $BATCH_ID} Pig parameter
     */
    public static void executeQueryScript(
            String scriptPath, String inputDir, String outputDir, int batchId)
            throws Exception {

        PigServer pig = PigServerManager.get();

        Map<String, String> params = new HashMap<>();
        params.put("INPUT_DIR", inputDir);
        params.put("OUTPUT_DIR", outputDir);
        params.put("BATCH_ID", String.valueOf(batchId));

        // registerScript compiles + runs the script synchronously.
        // PigServer translates it to a MapReduce job (local runner by default).
        pig.registerScript(scriptPath, params);
    }

    /**
     * Executes the parse-and-clean setup script via the embedded API.
     *
     * <p>Replaces the original {@code ProcessBuilder} call in
     * {@code PigDataSetupExecutor}:
     * <pre>
     *   pig -x local -param INPUT_FILE=... -param VALID_OUTPUT=... -param MALFORMED_OUTPUT=... -param BATCH_ID=... -f parse_and_clean.pig
     * </pre>
     *
     * @param scriptPath      path to parse_and_clean.pig
     * @param inputFile       value for {@code $INPUT_FILE}
     * @param validOutput     value for {@code $VALID_OUTPUT}
     * @param malformedOutput value for {@code $MALFORMED_OUTPUT}
     * @param batchId         value for {@code $BATCH_ID}
     */
    public static void executeSetupScript(
            String scriptPath, String inputFile,
            String validOutput, String malformedOutput, int batchId)
            throws Exception {

        PigServer pig = PigServerManager.get();

        Map<String, String> params = new HashMap<>();
        params.put("INPUT_FILE", inputFile);
        params.put("VALID_OUTPUT", validOutput);
        params.put("MALFORMED_OUTPUT", malformedOutput);
        params.put("BATCH_ID", String.valueOf(batchId));

        pig.registerScript(scriptPath, params);
    }

    // -------------------------------------------------------------------------
    // File-system helpers (unchanged from original)
    // -------------------------------------------------------------------------

    /**
     * Reads all non-hidden output lines from a Pig output directory.
     * Unchanged from the original implementation.
     */
    public static List<String> readOutputLines(String outputDir) throws IOException {
        List<String> lines = new ArrayList<>();
        File dir = new File(outputDir);
        if (!dir.exists() || !dir.isDirectory()) return lines;

        File[] files = dir.listFiles();
        if (files == null) return lines;

        for (File file : files) {
            if (file.isFile() && !file.getName().startsWith(".")) {
                try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        lines.add(line);
                    }
                }
            }
        }
        return lines;
    }

    /**
     * Recursively deletes a directory and its contents.
     * Unchanged from the original implementation.
     */
    public static void deleteDirectory(File dir) {
        if (!dir.exists()) return;
        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    deleteDirectory(file);
                } else {
                    file.delete();
                }
            }
        }
        dir.delete();
    }
}