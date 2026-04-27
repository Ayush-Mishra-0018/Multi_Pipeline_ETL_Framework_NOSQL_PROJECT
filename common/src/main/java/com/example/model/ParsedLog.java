package com.example.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ParsedLog {

    private String host;

    private String rawTimestamp;
    private String date;
    private int hour;

    private String method;
    private String path;
    private String protocol;

    private int status;
    private long bytes;

    private boolean malformed;

    private int batchId;
}