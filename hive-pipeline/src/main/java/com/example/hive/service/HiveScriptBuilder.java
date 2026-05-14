package com.example.hive.service;

import com.example.config.ConfigReader;

import java.io.File;

/**
 * HiveScriptBuilder
 *
 * Resolves paths to HQL script files and builds --hiveconf strings
 * that setup / query scripts reference via ${hiveconf:key}.
 *
 * No ETL logic.  No file content manipulation.
 */
public final class HiveScriptBuilder {

    private static final String SCRIPT_DIR =
            ConfigReader.get("hive.script.dir", "./hive-pipeline/scripts");

    private static final String HDFS_URI =
            ConfigReader.get("hdfs.uri", "hdfs://localhost:9000");

    private HiveScriptBuilder() {
    }

    // ──────────────────────────────────────────────────────────────────
    // Script path resolution
    // ──────────────────────────────────────────────────────────────────

    /**
     * Returns the absolute path to a named HQL script file.
     * Throws immediately if the file does not exist (fail-fast).
     */
    public static String getScriptPath(String scriptName) {

        File f = new File(SCRIPT_DIR, scriptName);

        if (!f.exists()) {
            throw new RuntimeException(
                    "HQL script not found: " + f.getAbsolutePath()
            );
        }

        return f.getAbsolutePath();
    }

    // ──────────────────────────────────────────────────────────────────
    // Hiveconf string builders
    // All returned strings are in the form "key=value" and are passed
    // to Hive via --hiveconf key=value on the command line.
    // ──────────────────────────────────────────────────────────────────

    /**
     * Returns "hdfsBase=hdfs://localhost:9000"
     * Referenced in HQL as ${hiveconf:hdfsBase}
     */
    public static String hdfsBaseConf() {
        return "hdfsBase=" + HDFS_URI;
    }

    /**
     * Returns "batchNHdfsPath=hdfs://localhost:9000/nasa_raw/batch_N/<filename>"
     *
     * The hive_setup.hql script references these as:
     *   ${hiveconf:batch1HdfsPath}
     *   ${hiveconf:batch2HdfsPath}
     *   etc.
     *
     * @param batchNum     1-based batch index (matches the batch_id column)
     * @param localFilePath local file path whose filename is reused on HDFS
     */
    public static String batchHdfsConf(int batchNum, String localFilePath) {

        // Extract only the filename (e.g. "access_log_Jul95")
        String filename = new File(localFilePath.trim()).getName();

        // Full HDFS URI to the uploaded file
        String hdfsPath = HDFS_URI
                + "/nasa_raw/batch_" + batchNum
                + "/" + filename;

        return "batch" + batchNum + "HdfsPath=" + hdfsPath;
    }

    /**
     * HDFS *directory* path for a given batch (no filename).
     * Used by HdfsUploader.upload() to know where to put the file.
     *
     * @param batchNum 1-based batch index
     */
    public static String hdfsDir(int batchNum) {
        return "/nasa_raw/batch_" + batchNum;
    }
}
