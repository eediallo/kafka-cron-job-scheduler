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
    private  final  String topic;

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

    /**
     * send a newly scheduled job to a cluster specified topic
     */
    public void sendJobTopic(String topicName, String jobId, String command, String cluster, int maxAttempts) {
        try {
            JobPayload payload = new JobPayload(jobId, command, cluster, Instant.now().toString(), maxAttempts);
            String jsonPayload = mapper.writeValueAsString(payload);

            // Random UUID key ensures messages are distributed across partitions
            String messageKey = UUID.randomUUID().toString();

            // Create producer record
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, messageKey, jsonPayload);
            producer.send(record, ((metadata, exception) -> {
                if (exception == null) {
                    log.info("Producer job [{}] -> Topic: {},  partition {}, Offset {}", payload.getJobId(), metadata.topic(), metadata.partition(), metadata.offset());
                } else {
                    log.error("Failed to push job [{}] to kafka", payload.getJobId(), exception);
                }
            }));
        } catch (Exception e) {
            log.error("Error building or serializing message payload for job [{}]", jobId, e);
        }
    }

    /**
     * Re-queues a failed job payload to the cluster retry topic
     */
    public void sentToRetryTopic(String retryTopic, JobPayload payload) {
        try {
            String jsonPayload = mapper.writeValueAsString(payload);
            ProducerRecord<String, String> record = new ProducerRecord<>(retryTopic, payload.getJobId(), jsonPayload);
            producer.send(record, ((metadata, exception) -> {
                if (exception == null) {
                    log.info("Re-queued job [{}] -> Retry Topic: {},  partition {}, Offset {}", payload.getJobId(), metadata.topic(), metadata.partition(), metadata.offset());
                } else {
                    log.error("Failed to re-queue job [{}] to retry topic: {}", payload.getJobId(), retryTopic, exception);
                }
            }));
        } catch (Exception e) {
            log.error("Error  serializing retry payload for job [{}]", payload.getJobId(), e);
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
