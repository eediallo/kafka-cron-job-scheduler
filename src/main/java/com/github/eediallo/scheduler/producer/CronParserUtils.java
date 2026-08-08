package com.github.eediallo.scheduler.producer;

public class CronParserUtils {
    public static String toQuartzCron(String standardCron) {
        String[] parts = standardCron.split("\\s+");

        if (parts.length == 5) {
            String dom = parts[2];
            String dow = parts[4];

            if (dom.equals("*") && dow.equals("*")) {
                dow = "?";
            } else if (!dom.equals("*") && dow.equals("*")) {
                dow = "?";
            } else if (dom.equals("*") && !dow.equals("*")) {
                dom = "?";
            }

            return String.format("0 %s %s %s %s %s", parts[0], parts[1], dom, parts[3], dow);
        }
        return standardCron;
    }
}
