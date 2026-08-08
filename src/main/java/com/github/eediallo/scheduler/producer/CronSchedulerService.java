package com.github.eediallo.scheduler.producer;

import com.github.eediallo.scheduler.CronParserUtils;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class CronSchedulerService {

    private final Scheduler scheduler;

    public CronSchedulerService(KafkaMessageProducer kafkaMessageProducer) throws SchedulerException {
        this.scheduler = StdSchedulerFactory.getDefaultScheduler();
        this.scheduler.getContext().put("kafkaProducer", kafkaMessageProducer);
    }

    private void scheduleLine(int lineNumber, String line) throws SchedulerException {
        String[] parts = line.split("\\s+", 8);

        if (parts.length < 8) {
            System.err.println("Skipping invalid crontab line: " + lineNumber + ": " + line);
            return;
        }

        String standardCron = String.join(" ", parts[0], parts[1], parts[2], parts[3], parts[4]);

        String targetCluster = parts[5].toLowerCase();

        int maxAttempts = Integer.parseInt(parts[6]);

        String command = parts[7];

        String quartzCron = CronParserUtils.toQuartzCron(standardCron);

        JobDetail job = JobBuilder.newJob(CronJob.class).withIdentity("job_" + lineNumber).usingJobData("lineNumber", String.valueOf(lineNumber)).usingJobData("cluster", targetCluster).usingJobData("maxAttempts", maxAttempts).usingJobData("command", command).build();

        Trigger trigger = TriggerBuilder.newTrigger().withIdentity("trigger_" + lineNumber).withSchedule(CronScheduleBuilder.cronSchedule(quartzCron)).build();

        scheduler.scheduleJob(job, trigger);
        System.out.println("Scheduled job " + lineNumber + " -> " + line);
    }

    public void loadAndScheduleJobs(String filepath) throws IOException, SchedulerException {
        try (BufferedReader reader = new BufferedReader(new FileReader(filepath))) {
            String line;
            int lineNumber = 0;
            while ((line = reader.readLine()) != null) {
                line = line.trim();

                if (!line.isEmpty() && !line.startsWith("#")) {
                    scheduleLine(lineNumber, line);
                }

                lineNumber++;
            }
        }
    }

    public void start() throws SchedulerException {
        scheduler.start();
    }

}
