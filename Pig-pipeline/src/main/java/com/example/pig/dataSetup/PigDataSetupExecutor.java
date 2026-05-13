package com.example.pig.dataSetup;

import com.example.config.ConfigReader;
import com.example.model.MalformedRecord;
import com.example.model.PipelineExecutionResult;
import com.example.pig.service.PigInsertService;
import com.example.postgres.service.PostgresInsertService;
import com.example.postgres.service.PostgresSchemaInitializer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public final class PigDataSetupExecutor {

    private static final String PIG_SCRIPT = "Pig-pipeline/src/main/resources/pig/parse_and_clean.pig";

    private PigDataSetupExecutor() {
    }

    public static PipelineExecutionResult execute() {

        long startTime = System.currentTimeMillis();

        PostgresSchemaInitializer.initialize("pig");

        boolean shouldClear = Boolean.parseBoolean(
                ConfigReader.get("mongo.clear.before.run", "true")
        );

        if (shouldClear) {
            PigInsertService.clearData();
        }

        String filePathsStr = ConfigReader.get("input.file.paths");
        String[] filePaths = filePathsStr.split(",");

        int batchSize = Integer.parseInt(
                ConfigReader.get("batch.size", "10000")
        );

        int batchId = 1;
        long totalRecordsProcessed = 0;
        long totalMalformed = 0;
        long totalValid = 0;
        int totalBatches = 0;

        List<MalformedRecord> malformedRecords = new ArrayList<>();

        try {
            for (String filePath : filePaths) {
                filePath = filePath.trim();
                System.out.println("Processing file: " + filePath);

                try (BufferedReader reader = Files.newBufferedReader(Path.of(filePath), StandardCharsets.ISO_8859_1)) {
                    while (true) {
                        List<String> rawLines = new ArrayList<>(batchSize);
                        String line;
                        while (rawLines.size() < batchSize && (line = reader.readLine()) != null) {
                            rawLines.add(line);
                        }

                        if (rawLines.isEmpty()) {
                            break;
                        }

                        // Write raw batch
                        String rawBatchFile = PigInsertService.writeRawBatch(rawLines, batchId);
                        
                        String validOutput = "./pig_data/valid/batch_" + batchId;
                        String malformedOutput = "./pig_data/malformed/batch_" + batchId;

                        // Execute Pig Script
                        executePigScript(rawBatchFile, validOutput, malformedOutput, batchId);

                        // Read Pig Outputs for Metadata
                        long validCount = countLinesInDirectory(validOutput);
                        List<String> malformedLines = readLinesFromDirectory(malformedOutput);

                        for (String mLine : malformedLines) {
                            malformedRecords.add(MalformedRecord.builder()
                                    .batchId(batchId)
                                    .line(mLine)
                                    .build());
                        }

                        totalRecordsProcessed += rawLines.size();
                        totalMalformed += malformedLines.size();
                        totalValid += validCount;
                        totalBatches++;

                        System.out.println("Batch " + batchId + " processed by Pig successfully. (Valid: " + validCount + ", Malformed: " + malformedLines.size() + ")");
                        batchId++;
                    }
                }
            }

            System.out.println("\nTotal malformed records: " + malformedRecords.size());

            long endTime = System.currentTimeMillis();
            long totalTime = endTime - startTime;
            double avgBatchSize = totalBatches == 0 ? 0 : (double) totalRecordsProcessed / totalBatches;

            org.bson.Document metadata = new org.bson.Document()
                    .append("totalRecords", (int) totalRecordsProcessed)
                    .append("totalValid", (int) totalValid)
                    .append("totalMalformed", (int) totalMalformed)
                    .append("totalBatches", totalBatches)
                    .append("avgBatchSize", avgBatchSize)
                    .append("executionTimeMs", (int) totalTime);

            PostgresInsertService.insertGlobalMetadata(
                    "Pig",
                    List.of(1, 2, 3), 
                    totalTime,
                    metadata
            );

            PostgresInsertService.insertMalformed("pig", malformedRecords);

            System.out.println("\nPig pipeline execution completed.");

            return PipelineExecutionResult.builder()
                    .executionTime(totalTime)
                    .malformedRecords(malformedRecords)
                    .build();

        } catch (Exception e) {
            e.printStackTrace();
        }

        return PipelineExecutionResult.builder()
                .executionTime(0)
                .malformedRecords(new ArrayList<>())
                .build();
    }

    private static void executePigScript(String inputFile, String validOutput, String malformedOutput, int batchId) throws Exception {
        String pigCommand = System.getenv("PIG_HOME") != null 
                ? System.getenv("PIG_HOME") + "/bin/pig" 
                : "/home/santhosh/.pig-0.17.0/bin/pig";
        
        ProcessBuilder pb = new ProcessBuilder(
                pigCommand,
                "-x", "local",
                "-param", "INPUT_FILE=" + inputFile,
                "-param", "VALID_OUTPUT=" + validOutput,
                "-param", "MALFORMED_OUTPUT=" + malformedOutput,
                "-param", "BATCH_ID=" + batchId,
                "-f", PIG_SCRIPT
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
            throw new RuntimeException("Pig script failed with exit code: " + exitCode);
        }
    }

    private static long countLinesInDirectory(String dirPath) throws IOException {
        long count = 0;
        File dir = new File(dirPath);
        if (!dir.exists() || !dir.isDirectory()) return 0;
        
        for (File file : dir.listFiles()) {
            if (file.isFile() && !file.getName().startsWith(".")) {
                try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                    while (reader.readLine() != null) {
                        count++;
                    }
                }
            }
        }
        return count;
    }

    private static List<String> readLinesFromDirectory(String dirPath) throws IOException {
        List<String> lines = new ArrayList<>();
        File dir = new File(dirPath);
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

    public static void main(String[] args) {
        execute();
    }
}
