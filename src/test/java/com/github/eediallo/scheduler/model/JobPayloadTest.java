package com.github.eediallo.scheduler.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class JobPayloadTest {
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void serializeAndDeserialized_maintainAllFields() throws  Exception {
        JobPayload  original = new JobPayload("job_99", "uptime", "cluster_a", "2026-08-08T10:00:00Z", 5, 2);

        String json = mapper.writeValueAsString(original);
        JobPayload deserialized = mapper.readValue(json, JobPayload.class);

        assertEquals(original.getJobId(), deserialized.getJobId());
        assertEquals(original.getCommand(), deserialized.getCommand());
        assertEquals(original.getCluster(), deserialized.getCluster());
        assertEquals(original.getScheduledAt(), deserialized.getScheduledAt());
        assertEquals(original.getMaxAttempts(), deserialized.getMaxAttempts());
        assertEquals(original.getRemainingAttempts(), deserialized.getRemainingAttempts());
    }

}