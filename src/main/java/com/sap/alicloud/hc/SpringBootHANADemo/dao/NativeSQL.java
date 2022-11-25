package com.sap.alicloud.hc.SpringBootHANADemo.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.sap.alicloud.hc.SpringBootHANADemo.model.JobExecutionStatus;
import com.sap.alicloud.hc.SpringBootHANADemo.model.dto.JobExecutionStatusDto;

@Component
public class NativeSQL {

    Logger logger = LoggerFactory.getLogger(NativeSQL.class);
    private Connection connection = null;
    @Autowired
    Database db;

    public void createConnection() {

        long startTime = System.currentTimeMillis();

        logger.info("Java version: " + com.sap.db.jdbc.Driver.getJavaVersion());
        logger.info("Minimum supported Java version and SAP driver version number: " + com.sap.db.jdbc.Driver.getVersionInfo());

        try {
            connection = DriverManager.getConnection(db.getUrl(), db.getUsername(), db.getPassword());

            if (connection != null) {
                logger.info("Connection to HANA successful!");
            }

            long endTime = System.currentTimeMillis();
            long executeTime = endTime - startTime;

            logger.info("HANA Connection Time:" + executeTime);

        }
        catch (SQLException e) {
            logger.error("Connection Failed:");
            logger.error(e.toString());
            return;
        }
    }

    public void insertJob(JobExecutionStatus job) {
        if (connection != null) {
            try {
                long startTime = System.currentTimeMillis();

                PreparedStatement pstmt = connection.prepareStatement(
                        "INSERT INTO \"ECM_JOB_EXECUTION_STATUS\" (\"JOB_ID\", \"JOB_NAME\", \"RESULT\", \"START_TIME\", \"STATUS\") VALUES (?, ?, ?, ?, ?)");
                pstmt.setNString(1, job.getJobId());
                pstmt.setNString(2, job.getJobName());
                pstmt.setNString(3, job.getResult());
                pstmt.setLong(4, job.getStartTime());
                pstmt.setNString(5, job.getStatus());

                pstmt.executeUpdate();

                long endTime = System.currentTimeMillis();
                long executeTime = endTime - startTime;

                logger.info("Insert Job:" + job.getJobId() + ":" + executeTime);

            }
            catch (SQLException e) {
                logger.error("Insert failed!");
                logger.error(e.toString());
            }
        }
    }

    public void updateJob(JobExecutionStatus job) {
        if (connection != null) {
            try {
                long startTime = System.currentTimeMillis();

                PreparedStatement pstmt = connection.prepareStatement(
                        "UPDATE \"ECM_JOB_EXECUTION_STATUS\" SET \"RESULT\" = ?, \"STATUS\" = ? WHERE (\"JOB_ID\" = ?)");
                pstmt.setNString(1, job.getResult());
                pstmt.setNString(2, job.getStatus());
                pstmt.setNString(3, job.getJobId());
                pstmt.executeUpdate();

                long endTime = System.currentTimeMillis();
                long executeTime = endTime - startTime;

                logger.info("Update Job:" + job.getJobId() + ":" + executeTime);

            }
            catch (SQLException e) {
                logger.error("Update failed!");
                logger.error(e.toString());
            }
        }
    }

    public JobExecutionStatusDto getJob(String jobId) throws SQLException {
        if (connection != null) {
            long startTime = System.currentTimeMillis();
            PreparedStatement pstmt = connection.prepareStatement(
                    "SELECT \"JOB_ID\", \"JOB_NAME\", \"RESULT\", \"START_TIME\", \"STATUS\" FROM \"ECM_JOB_EXECUTION_STATUS\" WHERE (\"JOB_ID\" = ?)");
            pstmt.setNString(1, jobId);

            ResultSet rs = pstmt.executeQuery();

            if (rs != null && rs.next()) {
                String job_id = rs.getString("JOB_ID");
                String job_name = rs.getString("JOB_Name");
                long start_time = rs.getLong("START_TIME");
                String status = rs.getString("STATUS");
                String result = rs.getString("RESULT");

                JobExecutionStatusDto job = createJobExecutionStatusDto(job_id, job_name, start_time, status, result);
                
                return job;
            }
            else {
                throw new RuntimeException("No job with job ID: " + jobId + " found.");
            }

        }
        else {
            throw new RuntimeException("Connection to HANA failed.");
        }
    }

    public List<JobExecutionStatusDto> getAllJobs(String sortOrder) throws SQLException, RuntimeException {
        List<JobExecutionStatusDto> jobs = new ArrayList<JobExecutionStatusDto>();
        if (connection != null) {
            long startTime = System.currentTimeMillis();

            PreparedStatement pstmt;

            if (sortOrder.equals("DESC")) {
                pstmt = connection.prepareStatement(
                        "SELECT \"JOB_ID\", \"JOB_NAME\", \"RESULT\", \"START_TIME\", \"STATUS\" FROM \"ECM_JOB_EXECUTION_STATUS\" ORDER BY \"START_TIME\" DESC");
            }
            else {
                pstmt = connection.prepareStatement(
                        "SELECT \"JOB_ID\", \"JOB_NAME\", \"RESULT\", \"START_TIME\", \"STATUS\" FROM \"ECM_JOB_EXECUTION_STATUS\" ORDER BY \"START_TIME\" ASC");
            }

            ResultSet rs = pstmt.executeQuery();

            if (rs != null) {
                while (rs.next()) {
                    String job_id = rs.getString("JOB_ID");
                    String job_name = rs.getString("JOB_Name");
                    long start_time = rs.getLong("START_TIME");
                    String status = rs.getString("STATUS");
                    String result = rs.getString("RESULT");

                    JobExecutionStatusDto job = createJobExecutionStatusDto(job_id, job_name, start_time, status, result);

                    logger.info(job_id + "-" + job_name + "-" + start_time + "-" + status + "-" + result);

                    jobs.add(job);
                }
            }

            long endTime = System.currentTimeMillis();
            long executeTime = endTime - startTime;

            logger.info("Query Jobs:" + executeTime);

            return jobs;

        }
        else {
            throw new RuntimeException("Connection to HANA failed.");
        }

    }

    private JobExecutionStatusDto createJobExecutionStatusDto(final String job_id, final String job_name, final long start_time,
            final String status, final String result) {
        JobExecutionStatus job = new JobExecutionStatus();
        job.setJobId(job_id);
        job.setJobName(job_name);
        job.setStartTime(start_time);
        job.setStatus(status);
        job.setResult(result);
        return new JobExecutionStatusDto(job);
    }

    public void closeConnection() {
        if (connection != null) {
            try {
                connection.close();
                connection = null;
            }
            catch (SQLException e) {
                e.printStackTrace();
            }

        }
    }

}