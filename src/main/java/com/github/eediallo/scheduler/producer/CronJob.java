package com.github.eediallo.scheduler.producer;

import org.quartz.*;

public class CronJob implements Job {

    @Override
    public void execute(JobExecutionContext context) {
        try {
            KafkaMessageProducer producer = (KafkaMessageProducer) context.getScheduler().getContext().get("kafkaProducer");

            Object rawlineNumber = context.getMergedJobDataMap().getString("lineNumber");
            String lineNumber = rawlineNumber != null ? String.valueOf(rawlineNumber) : "0";
            String command = String.valueOf(context.getMergedJobDataMap().get("command"));
            producer.sendJob("job_" + lineNumber, command);

        } catch (SchedulerException e) {
            throw new RuntimeException(e);
        }
    }
}
