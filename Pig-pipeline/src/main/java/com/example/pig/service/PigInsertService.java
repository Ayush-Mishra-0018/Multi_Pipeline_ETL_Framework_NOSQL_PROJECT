package com.example.pig.service;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public final class PigInsertService {

    private static final String PIG_DATA_DIR = "./pig_data";
    private static final String RAW_DIR = PIG_DATA_DIR + "/raw";

    private PigInsertService() {
    }

    public static void clearData() {
        deleteDirectory(new File(PIG_DATA_DIR));
        new File(RAW_DIR).mkdirs();
        System.out.println("Pig data directory cleared and recreated: " + PIG_DATA_DIR);
    }

    private static void deleteDirectory(File dir) {
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

    public static String writeRawBatch(List<String> rawLines, int batchId) {
        if (rawLines.isEmpty()) {
            return null;
        }

        File rawDir = new File(RAW_DIR);
        if (!rawDir.exists()) {
            rawDir.mkdirs();
        }

        String filePath = RAW_DIR + "/batch_" + batchId + ".txt";
        File file = new File(filePath);

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
            for (String line : rawLines) {
                writer.write(line);
                writer.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
        
        return filePath;
    }
}
