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

// Utility that executes Pig Latin scripts through the <em>embedded Pig API</em>

public final class PigScriptExecutor {

    private PigScriptExecutor() {}

    public static void executeQueryScript(
            String scriptPath, String inputDir, String outputDir, int batchId)
            throws Exception {

        int maxRetries = 5;
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                PigServer pig = PigServerManager.get();

                Map<String, String> params = new HashMap<>();
                params.put("INPUT_DIR", inputDir);
                params.put("OUTPUT_DIR", outputDir);
                params.put("BATCH_ID", String.valueOf(batchId));

                // registerScript compiles + runs the script synchronously.
                // PigServer translates it to a MapReduce job (local runner by default).
                pig.registerScript(scriptPath, params);
                return; // Success
            } catch (Exception e) {
                if (attempt == maxRetries) {
                    throw e;
                }
                String errorMsg = e.toString();
                if (errorMsg.contains("duplicate uid") || errorMsg.contains("PlanValidationException") || 
                    errorMsg.contains("FrontendException") || errorMsg.contains("SchemaTupleFrontend")) {
                    System.out.println("Concurrency conflict in Pig compiler for batch " + batchId + " (attempt " + attempt + "/" + maxRetries + "). Retrying...");
                    PigServerManager.close();
                    Thread.sleep(100 + (int)(Math.random() * 200));
                } else {
                    throw e;
                }
            }
        }
    }


    public static void executeSetupScript(
            String scriptPath, String inputFile,
            String validOutput, String malformedOutput, int batchId)
            throws Exception {

        int maxRetries = 5;
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                PigServer pig = PigServerManager.get();

                Map<String, String> params = new HashMap<>();
                params.put("INPUT_FILE", inputFile);
                params.put("VALID_OUTPUT", validOutput);
                params.put("MALFORMED_OUTPUT", malformedOutput);
                params.put("BATCH_ID", String.valueOf(batchId));

                pig.registerScript(scriptPath, params);
                return; // Success
            } catch (Exception e) {
                if (attempt == maxRetries) {
                    throw e;
                }
                String errorMsg = e.toString();
                if (errorMsg.contains("duplicate uid") || errorMsg.contains("PlanValidationException") || 
                    errorMsg.contains("FrontendException") || errorMsg.contains("SchemaTupleFrontend")) {
                    System.out.println("Concurrency conflict in Pig compiler for setup batch " + batchId + " (attempt " + attempt + "/" + maxRetries + "). Retrying...");
                    PigServerManager.close();
                    Thread.sleep(100 + (int)(Math.random() * 200));
                } else {
                    throw e;
                }
            }
        }
    }


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

    // Recursively deletes a directory and its contents.
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