package com.github.eediallo.scheduler.producer;

import org.quartz.SchedulerException;

import java.io.IOException;

public class Main {

    public static void main(String[] args) {
        String filePath = args.length > 0 ? args[0] : "crontab.txt";
        String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092");
        String topic = System.getenv("KAFKA_TOPIC");
        try {
            KafkaMessageProducer producer = new KafkaMessageProducer(bootstrapServers, topic);
            CronSchedulerService schedulerService = new CronSchedulerService(producer);

            schedulerService.loadAndScheduleJobs(filePath);
            schedulerService.start();

            System.out.println("Cron Scheduler Running... Ctrl + c to exit");

            // prevent JVM from exiting immediately
            Thread.currentThread().join();

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
