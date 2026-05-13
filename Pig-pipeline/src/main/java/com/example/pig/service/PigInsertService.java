package com.example.pig.service;

import com.example.model.BatchResult;
import com.example.model.ParsedLog;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public final class PigInsertService {

    private static final String PIG_DATA_DIR = "./pig_data";
    private static final String PARSED_LOGS_FILE = PIG_DATA_DIR + "/parsed_logs";

    private PigInsertService() {
    }

    public static void clearData() {
        File dir = new File(PIG_DATA_DIR);
        if (dir.exists()) {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File file : files) {
                    file.delete();
                }
            }
            dir.delete();
        }
        dir.mkdirs();
        System.out.println("Pig data directory cleared and recreated: " + PIG_DATA_DIR);
    }

    public static void insertParsedLogs(BatchResult result, int batch_id) {
        List<ParsedLog> logs = result.getParsedLogs();
        if (logs.isEmpty()) {
            return;
        }

        File file = new File(PARSED_LOGS_FILE + "_batch_" + batch_id + ".tsv");
        boolean isNewFile = !file.exists();
        System.out.println("File" + (isNewFile ? " (New)" : "(Old)"));

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file, true))) {
            for (ParsedLog log : logs) {
                if (!log.isMalformed()) {
                    writer.write(String.format("%s\t%s\t%s\t%d\t%s\t%s\t%s\t%d\t%d\t%d\n",
                            log.getHost(),
                            log.getRawTimestamp(),
                            log.getDate(),
                            log.getHour(),
                            log.getMethod(),
                            log.getPath(),
                            log.getProtocol(),
                            log.getStatus(),
                            log.getBytes(),
                            log.getBatchId()
                    ));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
