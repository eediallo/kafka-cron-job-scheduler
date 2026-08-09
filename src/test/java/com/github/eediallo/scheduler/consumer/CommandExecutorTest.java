package com.github.eediallo.scheduler.consumer;

import com.github.eediallo.scheduler.model.JobPayload;
import com.github.eediallo.scheduler.producer.KafkaMessageProducer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@DisplayName("CommandExecutor - Execution & Retry Handling")
class CommandExecutorTest {

    private KafkaMessageProducer mockProducer;
    private CommandExecutor executor;

    @BeforeEach
    void setup() {
        mockProducer = Mockito.mock(KafkaMessageProducer.class);
        executor = new CommandExecutor(mockProducer);
    }

    @Test
    @DisplayName("Should not trigger retry on successful execution")
    void execute_successfulCommand_doesNotTriggerRetry() {
        JobPayload jobPayload = new JobPayload("job_9", "echo 'hello world'", "cluster_a", "2026-08-08T10:00:00Z", 3, 3);

        executor.execute("cron-jobs-cluster-a", "worker-1", jobPayload, 0);

        verify(mockProducer, never()).sentToRetryTopic(any(), any());
    }


    @Test
    @DisplayName("Should not execute when command is empty")
    void execute_emptyCommand_skipsExecution() {
        JobPayload jobPayload = new JobPayload("job_9", "   ", "cluster_a", "2026-08-08T10:00:00Z", 3, 3);

        executor.execute("cron-jobs-cluster-a", "worker-1", jobPayload, 0);

        verify(mockProducer, never()).sentToRetryTopic(any(), any());
    }


    @Test
    @DisplayName("Should decrement remaining attempts and publish to retry topic when job fails")
    void execute_failingCommand_decrementsAttemptsAndRequeue() {
        JobPayload jobPayload = new JobPayload("job_9", "exit 1", "cluster_a", "2026-08-08T10:00:00Z", 3, 3);
        ArgumentCaptor<JobPayload> payloadArgumentCaptor = ArgumentCaptor.forClass(JobPayload.class);

        executor.execute("cron-jobs-cluster-a", "worker-1", jobPayload, 0);

        verify(mockProducer).sentToRetryTopic(eq("cron-jobs-cluster-a-retry"), payloadArgumentCaptor.capture());
        assertEquals(2, payloadArgumentCaptor.getValue().getRemainingAttempts(), "Expected remainingAttempts to decrement from 3 to 2 on first failure");
    }

    @Test
    @DisplayName("Should discard job and NOT publish to retry topic when remaining attempts reach zero")
    void execute_failingCommand_exhaustedAttempts_discardsJobWithoutRequeue() {
        JobPayload jobPayload = new JobPayload("job_9", "exit 1", "cluster_a", "2026-08-08T10:00:00Z", 3, 1);

        executor.execute("cron-jobs-cluster-a", "worker-1", jobPayload, 0);

        verify(mockProducer, never()).sentToRetryTopic(any(), any());
    }
}