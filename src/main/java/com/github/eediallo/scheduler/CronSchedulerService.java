package com.github.eediallo.scheduler;

import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class CronSchedulerService {

    private final Scheduler scheduler;

    public CronSchedulerService() throws SchedulerException {
        this.scheduler = StdSchedulerFactory.getDefaultScheduler();
    }

    private void scheduleLine(int lineNumber, String cronSpec) throws SchedulerException {
        String quartzCron = CronParserUtils.toQuartzCron(cronSpec);

        JobDetail job = JobBuilder.newJob(CronJob.class).withIdentity("job_" + lineNumber).usingJobData("lineNumber", lineNumber).build();

        Trigger trigger = TriggerBuilder.newTrigger().withIdentity("trigger_" + lineNumber).withSchedule(CronScheduleBuilder.cronSchedule(quartzCron)).build();

        scheduler.scheduleJob(job, trigger);
        System.out.println("Scheduled job " + lineNumber + " -> " + cronSpec);
    }

    public void loadAndScheduleJobs(String filepath) throws IOException, SchedulerException {
        try (BufferedReader reader = new BufferedReader(new FileReader(filepath))) {
            String line;
            int lineNumber = 0;
            while ((line = reader.readLine()) != null) {
                line.trim();

                if (!line.isEmpty() || !line.startsWith("#")) {
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
