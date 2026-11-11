package com.github.eediallo.scheduler.model;

public class JobPayload {
    private String jobId;
    private String command;
    private String cluster;
    private String scheduledAt;
    private int maxAttempts;
    private int remainingAttempts;

    public JobPayload() {
    }

    public JobPayload(String jobId, String command, String cluster, String scheduledAt, int maxAttempts, int remainingAttempts) {
        this.jobId = jobId;
        this.command = command;
        this.cluster = cluster;
        this.scheduledAt = scheduledAt;
        this.maxAttempts = maxAttempts;
        this.remainingAttempts = remainingAttempts;
    }

    public JobPayload(String jobId, String command, String cluster, String scheduledAt, int maxAttempts) {
        this(jobId, command, cluster, scheduledAt, maxAttempts, maxAttempts);
    }

    public String getJobId() {
        return jobId;
    }

    public String getCommand() {
        return command;
    }

    public String getScheduledAt() {
        return scheduledAt;
    }

    public String getCluster() {
        return cluster;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public int getRemainingAttempts() {
        return remainingAttempts;
    }

    public void setMaxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    public void setRemainingAttempts(int remainingAttempts) {
        this.remainingAttempts = remainingAttempts;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public void setCommand(String command) {
        this.command = command;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    public void setScheduledAt(String scheduledAt) {
        this.scheduledAt = scheduledAt;
    }
}
