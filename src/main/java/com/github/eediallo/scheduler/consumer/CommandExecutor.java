package com.github.eediallo.scheduler.consumer;

import com.github.eediallo.scheduler.model.JobPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommandExecutor {
    private final Logger log = LoggerFactory.getLogger(CommandExecutor.class);

    public void execute(String consumerId, JobPayload jobPayload, int partition) {
        log.info("[{}] RECEIVED JOB -> ID: {}, Command: '{}' , ScheduledAt: '{}' Partition: [Partition: {}]", consumerId,  jobPayload.getJobId(), jobPayload.getCommand(), jobPayload.getScheduledAt(), partition);
    }
}
