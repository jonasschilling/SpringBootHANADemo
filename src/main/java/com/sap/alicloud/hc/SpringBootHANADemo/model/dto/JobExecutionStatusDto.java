package com.sap.alicloud.hc.SpringBootHANADemo.model.dto;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;

import com.sap.alicloud.hc.SpringBootHANADemo.model.JobExecutionStatus;

public class JobExecutionStatusDto {

    private String jobId;
    private String jobName;
    private OffsetDateTime startDateTime;
    private String status;
    private String result;

    public JobExecutionStatusDto(JobExecutionStatus job) {
        this.jobId = job.getJobId();
        this.jobName = job.getJobName();
        this.startDateTime = convertStartTimeToDate(job.getStartTime());
        this.status = job.getStatus();
        this.result = job.getResult();
    }

    public String getJobId() {
        return jobId;
    }

    public String getJobName() {
        return jobName;
    }

    public String getStatus() {
        return status;
    }

    public String getResult() {
        return result;
    }

    public OffsetDateTime getStartDateTime() {
        return startDateTime;
    }

    private OffsetDateTime convertStartTimeToDate(long startTimeAsLong) {
        return OffsetDateTime.ofInstant(Instant.ofEpochMilli(startTimeAsLong), ZoneId.systemDefault());
    }
}
