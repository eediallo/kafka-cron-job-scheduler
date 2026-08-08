package com.github.eediallo.scheduler.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.eediallo.scheduler.model.JobPayload;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerService {
    private final Logger log = LoggerFactory.getLogger(ConsumerService.class);
    private final ObjectMapper mapper = new ObjectMapper();

    private final KafkaConsumer consumer;
    private final CommandExecutor executor;
    private String consumerId;
    private final String topic;
    private volatile boolean running = true;

    public ConsumerService(String bootstrapServices, String topic, String consumerGroup, String consumerId) {
        this.topic = topic;
        this.consumerId = consumerId;
        this.executor = new CommandExecutor();

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServices);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());


        // CREATE CONSUMER
        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(Collections.singletonList(topic));
    }

    public void start() {
        log.info("[{}] Consumer service starte. Listening for scheduled jobs...", consumerId);

        try {
            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        JobPayload payload = mapper.readValue(record.value(), JobPayload.class);
                        executor.execute(topic, consumerId, payload, record.partition());

                    } catch (Exception e) {
                        log.error("[{}] Error deserializing message on partition", consumerId, record.partition());
                    }
                }
            }
        } finally {
            consumer.close();
            log.info("[{}] Consumer service shut down gracefully.", consumerId);
        }
    }

    public void stop() {
        this.running = false;
    }
}
