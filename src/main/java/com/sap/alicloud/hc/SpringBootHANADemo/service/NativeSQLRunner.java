package com.sap.alicloud.hc.SpringBootHANADemo.service;

import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import com.sap.alicloud.hc.SpringBootHANADemo.dao.NativeSQL;
import com.sap.alicloud.hc.SpringBootHANADemo.model.JobExecutionStatus;
import com.sap.alicloud.hc.SpringBootHANADemo.model.dto.JobExecutionStatusDto;

@Service
public class NativeSQLRunner {

    private static int MAX_RUM = 100;

    @Autowired
    NativeSQL nativeSQL;

    @Async
    public void startTest() throws SQLException {
        nativeSQL.createConnection();
        for (int i = 0; i < MAX_RUM; i++) {
            JobExecutionStatus job = new JobExecutionStatus();
            job.setJobId(UUID.randomUUID().toString());
            job.setJobName("InstanceCreationJob");
            job.setResult("");
            job.setStartTime(System.currentTimeMillis());
            job.setStatus("In Progress");

            nativeSQL.insertJob(job);

            job.setResult("Instance creation successfully completed");
            job.setStatus("Succeeded");

            nativeSQL.updateJob(job);

            nativeSQL.getJob(job.getJobId());
        }
        nativeSQL.closeConnection();
    }

    @Async
    public JobExecutionStatusDto getJobById(String jobId) throws SQLException {
        nativeSQL.createConnection();
        JobExecutionStatusDto job = nativeSQL.getJob(jobId);
        nativeSQL.closeConnection();
        return job;
    }

    @Async
    public List<JobExecutionStatusDto> getAllJobs(String sortOrder) throws SQLException {
        nativeSQL.createConnection();
        List<JobExecutionStatusDto> jobs = nativeSQL.getAllJobs(sortOrder);
        nativeSQL.closeConnection();
        return jobs;
    }

}