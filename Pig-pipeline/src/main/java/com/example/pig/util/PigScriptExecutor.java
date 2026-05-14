package com.example.pig.util;

import com.example.config.ConfigReader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public final class PigScriptExecutor {

    private PigScriptExecutor() {
    }

    public static void executeQueryScript(String scriptPath, String inputDir, String outputDir, int batchId) throws Exception {
        String pigCommand = System.getenv("PIG_HOME") != null 
                ? System.getenv("PIG_HOME") + "/bin/pig" 
                : ConfigReader.get("pig.executable.path", "pig");
        
        ProcessBuilder pb = new ProcessBuilder(
                pigCommand,
                "-x", "local",
                "-param", "INPUT_DIR=" + inputDir,
                "-param", "OUTPUT_DIR=" + outputDir,
                "-param", "BATCH_ID=" + batchId,
                "-f", scriptPath
        );
        
        pb.redirectErrorStream(true);
        
        String javaHome = System.getProperty("java.home");
        if (javaHome != null) {
            pb.environment().put("JAVA_HOME", javaHome);
        }
        
        Process process = pb.start();
        
        try (BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = br.readLine()) != null) {
                // System.out.println(line); // Un-comment to see Pig logs
            }
        }
        
        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new RuntimeException("Pig script " + scriptPath + " failed with exit code: " + exitCode);
        }
    }

    public static List<String> readOutputLines(String outputDir) throws IOException {
        List<String> lines = new ArrayList<>();
        File dir = new File(outputDir);
        if (!dir.exists() || !dir.isDirectory()) return lines;
        
        for (File file : dir.listFiles()) {
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
    
    public static void deleteDirectory(File dir) {
        if (dir.exists()) {
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
}
