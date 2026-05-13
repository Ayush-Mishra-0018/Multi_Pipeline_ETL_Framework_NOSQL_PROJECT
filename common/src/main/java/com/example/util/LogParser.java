package com.example.util;

import com.example.model.ParsedLog;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class LogParser {

    private static final Pattern LOG_PATTERN = Pattern.compile(
            "^(\\S+) \\S+ \\S+ \\[(.*?)\\] \"(.*?)\" (\\d{3}) (\\S+)$"
    );

    private static final String UNKNOWN_BYTES = "-";

    private static final DateTimeFormatter INPUT_DATE_FORMAT =
            DateTimeFormatter.ofPattern(
                    "dd/MMM/yyyy",
                    Locale.ENGLISH
            );

    private static final DateTimeFormatter OUTPUT_DATE_FORMAT =
            DateTimeFormatter.ISO_LOCAL_DATE;

    private LogParser() {
    }

    public static ParsedLog parse(
            String line,
            int batchId
    ) {

        ParsedLog log =
                new ParsedLog();

        log.setBatchId(batchId);

        try {

            Matcher matcher =
                    LOG_PATTERN.matcher(line);

            if (!matcher.matches()) {

                System.out.println(
                        "Invalid log line: " + line
                );

                return malformed(log);
            }

            String host =
                    matcher.group(1);

            String rawTimestamp =
                    matcher.group(2);

            String request =
                    matcher.group(3);

            String status =
                    matcher.group(4);

            String bytes =
                    matcher.group(5);

            log.setHost(host);

            log.setRawTimestamp(rawTimestamp);

            parseDateAndHour(
                    log,
                    rawTimestamp
            );

            parseRequest(
                    log,
                    request
            );

            log.setStatus(
                    Integer.parseInt(status)
            );

            log.setBytes(
                    parseBytes(bytes)
            );

            log.setMalformed(false);

            return log;

        } catch (Exception e) {

            return malformed(log);
        }
    }

    private static ParsedLog malformed(
            ParsedLog log
    ) {

        log.setMalformed(true);

        return log;
    }

    private static void parseDateAndHour(
            ParsedLog log,
            String rawTimestamp
    ) {

        String[] parts =
                rawTimestamp.split(":");

        String rawDate =
                parts[0];

        int hour =
                Integer.parseInt(parts[1]);

        LocalDate date =
                LocalDate.parse(
                        rawDate,
                        INPUT_DATE_FORMAT
                );

        log.setDate(
                date.format(
                        OUTPUT_DATE_FORMAT
                )
        );

        log.setHour(hour);
    }

    private static void parseRequest(
            ParsedLog log,
            String request
    ) {

        String[] req =
                request.split(" ");

        if (req.length >= 3) {

            log.setMethod(req[0]);

            log.setPath(req[1]);

            log.setProtocol(req[2]);

        } else {

            log.setMalformed(true);
        }
    }

    private static long parseBytes(
            String bytes
    ) {

        return UNKNOWN_BYTES.equals(bytes)
                ? 0
                : Long.parseLong(bytes);
    }
}