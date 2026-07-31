package com.github.eediallo.scheduler;

import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class Main {

    public static void main(String[] args) throws Exception {
        String filePath = args.length > 0 ? args[0] : "crontab.txt";
        Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();

        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            int lineNumber = 0;

            while ((line = reader.readLine()) != null) {
                line = line.trim();

                // skip empty lines or comments
                if (line.isEmpty() || line.startsWith("#")) {
                    lineNumber++;
                    continue;
                }

                // Job Definition
                JobDetail job = JobBuilder.newJob(CronJob.class)
                        .withIdentity("job_" + lineNumber)
                        .usingJobData("lineNumber", lineNumber)
                        .build();

                // Trigger Definition
                Trigger trigger = TriggerBuilder.newTrigger()
                        .withIdentity("trigger_" + lineNumber)
                        .withSchedule(CronScheduleBuilder.cronSchedule(CronParserUtils.toQuartzCron(line)))
                        .build();

                // Schedule job
                scheduler.scheduleJob(job, trigger);
                System.out.println("Scheduled Job " + lineNumber + " with schedule: " + line);
                lineNumber++;

            }
            scheduler.start();
            System.out.println("Cron Scheduler Running... Ctrl + c to exit");
        } catch (IOException e) {
            System.err.println("Could not read crontab file: " + e.getMessage());
            System.exit(1);
        }
    }
}
