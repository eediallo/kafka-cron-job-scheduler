package com.github.eediallo.scheduler.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private final Logger log = LoggerFactory.getLogger(Main.class);
    private final ObjectMapper mapper = new ObjectMapper();


    public static void main(String[] args) {
        String bootStrapServer = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        String topic = System.getenv().getOrDefault("KAFKA_TOPIC", "cron-jobs");
        String consumerId = System.getenv().getOrDefault("CONSUMER_ID", "Consumer-Default");
        String consumerGroup = System.getenv().getOrDefault("CONSUMER_GROUP", "cron-execution-group");

        try {
            ConsumerService consumerService = new ConsumerService(bootStrapServer, topic, consumerGroup, consumerId);

            // Shutdown hook for graceful exit
            Runtime.getRuntime().addShutdownHook(new Thread(consumerService::stop));

            System.out.println("[" + consumerId + "] Starting Cron Consumer... Press Crl + C to exit. ");
            consumerService.start();

        } catch (Exception e) {
            System.err.println("[" + consumerId + "] Error Consumer service" + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }

    }
}
