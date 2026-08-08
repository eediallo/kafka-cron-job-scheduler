package com.github.eediallo.scheduler;

import org.quartz.SchedulerException;

import java.io.IOException;

public class Main {

    public static void main(String[] args) {
        String filePath = args.length > 0 ? args[0] : "crontab.txt";
        try {
            CronSchedulerService schedulerService = new CronSchedulerService();
            schedulerService.loadAndScheduleJobs(filePath);
            schedulerService.start();
            System.out.println("Cron Scheduler Running... Ctrl + c to exit");
        } catch (IOException e) {
            System.err.println("File Error: Could not read crontab file at '" + filePath + "' ." + e.getMessage());
            System.exit(1);
        } catch (SchedulerException e) {
            System.err.println("Scheduler Error: Quartz engine failed to start. " + e.getMessage());
            System.exit(1);
        } catch (Exception e) {
            System.err.println("Unexpected Error: " + e.getMessage());
            System.exit(1);
        }
    }
}
