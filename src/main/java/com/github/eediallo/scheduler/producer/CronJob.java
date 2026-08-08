package com.github.eediallo.scheduler.producer;

import org.quartz.*;

public class CronJob implements Job {

    @Override
    public void execute(JobExecutionContext context) {
        try {
            KafkaMessageProducer producer = (KafkaMessageProducer) context.getScheduler().getContext().get("kafkaProducer");

            Object rawLineNumber = context.getMergedJobDataMap().getString("lineNumber");
            String lineNumber = rawLineNumber != null ? String.valueOf(rawLineNumber) : "0";

            String cluster = context.getMergedJobDataMap().getString("cluster");
            if(cluster == null || cluster.trim().isEmpty()) {
                cluster = "cluster-a"; // fallback default when cluster is not specified
            }

            String topicNam  = "cron=jobs-" + cluster.toLowerCase().trim();

            String jobId = "job_" + lineNumber;

            String command = String.valueOf(context.getMergedJobDataMap().get("command"));
            producer.sendJobTopic(topicNam, jobId, command, cluster);

        } catch (SchedulerException e) {
            throw new RuntimeException(e);
        }
    }
}
