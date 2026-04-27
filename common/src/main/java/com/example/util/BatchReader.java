package com.example.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.nio.charset.StandardCharsets;

public class BatchReader implements AutoCloseable {

    private final BufferedReader reader;

    public BatchReader(String filePath) throws IOException {
        this.reader = Files.newBufferedReader(
                Path.of(filePath),
                StandardCharsets.ISO_8859_1
        );
    }

    public List<String> readNextBatch(int batchSize) throws IOException {

        List<String> lines = new ArrayList<>(batchSize);

        String line;

        while (lines.size() < batchSize &&
                (line = reader.readLine()) != null) {

            lines.add(line);
        }

        return lines;
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}