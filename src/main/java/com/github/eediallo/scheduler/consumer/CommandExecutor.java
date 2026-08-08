package com.github.eediallo.scheduler.consumer;

import com.github.eediallo.scheduler.model.JobPayload;
import com.github.eediallo.scheduler.producer.KafkaMessageProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;

public class CommandExecutor {
    private final Logger log = LoggerFactory.getLogger(CommandExecutor.class);
    private KafkaMessageProducer producer;

    public CommandExecutor(KafkaMessageProducer producer) {
        this.producer = producer;
    }

    public void execute(String topic, String consumerId, JobPayload jobPayload, int partition) {
        String command = jobPayload.getCommand();
        log.info("""
                        
                        ==================================================
                        [{}] RECEIVED JOB
                        ==================================================
                          Topic:        {}
                          Cluster:      {}
                          Job ID:       {}
                          Command:      {}
                          Scheduled At: {}
                          Partition:    {}
                          Remaining Attemps: {}
                        ==================================================""",
                consumerId,
                topic,
                jobPayload.getCluster(),
                jobPayload.getJobId(),
                jobPayload.getCommand(),
                jobPayload.getScheduledAt(),
                partition,
                jobPayload.getRemainingAttempts()
        );
        if (command == null || command.trim().isEmpty()) {
            log.warn("[{}] Empty command received for job {}, skipping execution.", consumerId, jobPayload.getJobId());
            return;
        }

        // Run command via bin/sh/ -c to support pipes, redirects and shell built-ins
        try {
            ProcessBuilder processBuilder = new ProcessBuilder("/bin/sh", "-c", command);
            processBuilder.redirectErrorStream(true); // merge stderr into stdout

            Process process = processBuilder.start();

            StringBuilder output = new StringBuilder();

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    output.append(line).append("\n");
                }
            }

            // wait up to 30 seconds for the process to complete
            boolean finished = process.waitFor(30, TimeUnit.SECONDS);
            if (!finished) {
                process.destroyForcibly();
                log.error("[{}] TIMED OUT -> JobId: {}", consumerId, jobPayload.getJobId());
                return;
            }

            int exitCode = process.exitValue();
            if (exitCode != 0) {
                int remainingAttempts = jobPayload.getRemainingAttempts() - 1;
                jobPayload.setRemainingAttempts(remainingAttempts);

                if (remainingAttempts > 0) {
                    String retryTopic = topic.endsWith("-retry") ? topic : topic + "-retry";
                    log.warn("[{}] JOB FAILED (ExitCode: {}) -> Re-queuing to retry Topic: {}. Remaining Attempts: {}",
                            consumerId, exitCode, retryTopic, remainingAttempts);
                    producer.sentToRetryTopic(retryTopic, jobPayload);
                } else {
                    log.error("[{}] JOB FAILED PERMANENTLY -> job ID: {} exhausted all all attempts. Discarding.", consumerId, jobPayload.getJobId());
                }

                return;
            }
            String resultOutput = output.toString().trim();
            log.info("""
                            
                            ==================================================
                            [{}] COMPLETED JOB
                            ==================================================
                              Topic:        {}
                              Cluster:      {}
                              Job ID:       {}
                              Exit Code:    {}
                            --------------------------------------------------
                            OUTPUT:
                            {}
                            ==================================================""",
                    consumerId,
                    topic,
                    jobPayload.getCluster(),
                    jobPayload.getJobId(),
                    exitCode,
                    resultOutput
            );
        } catch (Exception e) {
            log.error("[{}] FAILED TO EXECUTE -> jobID: {}", consumerId, jobPayload.getJobId(), e);
        }
    }
}
