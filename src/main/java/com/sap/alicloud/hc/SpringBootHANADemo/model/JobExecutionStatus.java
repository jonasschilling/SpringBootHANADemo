package com.sap.alicloud.hc.SpringBootHANADemo.model;
import java.time.OffsetDateTime;
import javax.persistence.*;
@Entity
@Table(name = "\"ECM_JOB_EXECUTION_STATUS\"")
public class JobExecutionStatus {
    @Id
    @Column(name = "\"JOB_ID\"", length = 200)
    private String jobId;
    @Column(name = "\"JOB_NAME\"", length = 190)
    private String jobName;
    @Column(name = "\"START_TIME\"")
    private long startTime;
    @Column(name = "\"STATUS\"", length = 16)
    private String status;
    @Column(name = "\"RESULT\"", length = 250)
    private String result;

    private OffsetDateTime startTimeAsDate;

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getResult() {
        return result;
    }

    public void setResult(String result) {
        this.result = result;
    }

}