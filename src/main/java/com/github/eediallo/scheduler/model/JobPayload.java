package com.github.eediallo.scheduler.model;

public class JobPayload {
    private String jobId;
    private String command;
    private String cluster;
    private String scheduledAt;


    public JobPayload() {
    }

    public JobPayload(String jobId, String command, String cluster, String scheduledAt) {
        this.jobId = jobId;
        this.command = command;
        this.cluster = cluster;
        this.scheduledAt = scheduledAt;
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
}
