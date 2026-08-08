package com.github.eediallo.scheduler.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.eediallo.scheduler.model.JobPayload;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Properties;
import java.util.UUID;

public class KafkaMessageProducer implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(KafkaMessageProducer.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    private final KafkaProducer<String, String> producer;
    private final String topic;

    public KafkaMessageProducer(String bootstrapSevers, String topic) {
        this.topic = topic;

        // set up properties
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapSevers);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Instantiates producer
        this.producer = new KafkaProducer<>(props);
    }

    public void sendJob(String jobId, String command) {
        try {
            JobPayload payload = new JobPayload(jobId, command, Instant.now().toString());
            String jsonPayload = mapper.writeValueAsString(payload);

            // Random UUID key ensures messages are distributed across partitions
            String messageKey = UUID.randomUUID().toString();

            // Create producer record
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, messageKey, jsonPayload);
            producer.send(record, ((metadata, exception) -> {
                if (exception == null) {
                    log.info("Producer job [{}] -> partition {}, Offset {}", jobId, metadata.partition(), metadata.offset());
                } else {
                    log.error("Failed to push job [{}] to kafka", jobId, exception);
                }
            }));
        } catch (Exception e) {
            log.error("Error building or serializing message payload for job [{}]", jobId, e);
        }
    }


    @Override
    public void close() {
        if (producer != null) {
            producer.flush();
            producer.close();
            log.info("Kafka producer closed gracefully.");
        }
    }
}
