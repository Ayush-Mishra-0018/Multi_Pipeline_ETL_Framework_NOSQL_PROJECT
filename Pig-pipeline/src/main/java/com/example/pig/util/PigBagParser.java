package com.example.pig.util;

import java.util.ArrayList;
import java.util.List;

public final class PigBagParser {

    private PigBagParser() {
    }

    /**
     * Parses a Pig bag string e.g. "{(host1),(host2)}" into a List of Strings: ["host1", "host2"]
     */
    public static List<String> parseBag(String bagStr) {
        List<String> result = new ArrayList<>();
        if (bagStr == null || bagStr.isEmpty() || bagStr.equals("{}")) {
            return result;
        }

        // Remove the outer { and }
        String inner = bagStr.substring(1, bagStr.length() - 1);
        
        // Split by "),"
        String[] parts = inner.split("\\),\\(");
        for (String part : parts) {
            // Remove lingering ( or )
            part = part.replace("(", "").replace(")", "");
            if (!part.isEmpty()) {
                result.add(part);
            }
        }
        return result;
    }
}
