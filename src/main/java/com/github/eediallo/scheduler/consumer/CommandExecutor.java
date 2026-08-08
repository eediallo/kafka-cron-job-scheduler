package com.github.eediallo.scheduler.consumer;

import com.github.eediallo.scheduler.model.JobPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;

public class CommandExecutor {
    private final Logger log = LoggerFactory.getLogger(CommandExecutor.class);

    public void execute(String consumerId, JobPayload jobPayload, int partition) {
        String command = jobPayload.getCommand();
        log.info("[{}] RECEIVED JOB -> ID: {}, Command: '{}' , ScheduledAt: '{}' Partition: [Partition: {}]", consumerId, jobPayload.getJobId(), jobPayload.getCommand(), jobPayload.getScheduledAt(), partition);

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
            String resultOutput = output.toString().trim();
            log.info("[{}] COMPLETED -> JobId: {}\n, ExitCode: {}\n---OUTPUT ---\n{} --------", consumerId, jobPayload.getJobId(), exitCode, resultOutput);

        } catch (Exception e) {
            log.error("[{}] FAILED TO EXECUTE -> jobID: {}", consumerId, jobPayload.getJobId(), e);
        }
    }
}
