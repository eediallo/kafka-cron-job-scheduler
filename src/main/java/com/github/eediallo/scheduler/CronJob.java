package com.github.eediallo.scheduler;

import org.quartz.*;

public class CronJob implements Job {

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        // Read line number passed in job details
        int lineNumber = context.getJobDetail().getJobDataMap().getInt("lineNumber");
        System.out.println("Running job " + lineNumber);
    }
}
