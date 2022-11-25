package com.sap.alicloud.hc.SpringBootHANADemo.controller;

import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.sap.alicloud.hc.SpringBootHANADemo.model.dto.JobExecutionStatusDto;
import com.sap.alicloud.hc.SpringBootHANADemo.service.NativeSQLRunner;

@CrossOrigin
@RestController
@RequestMapping("/job")
public class TestController {

    @Autowired
    NativeSQLRunner runner_native_sql;

    /*@RequestMapping("/upCheck")
    public String upCheck() {
        if(runner_native_sql.isDatabaseConnectionSuccessful()) {
            return "Database connection successful.";
        } else {
            return "Database connection failed.";
        }
    }*/

    /*@RequestMapping("/test_native_sql")
    public String test_native_sql() {
        runner_native_sql.startTest();
        return "Test Native SQL Started!";
    }*/

    @RequestMapping(value = "/all", method = RequestMethod.GET)
    public ResponseEntity getAllWithOrder(@RequestParam(required = false) String sortOrder) {
        try {
            if (Objects.isNull(sortOrder) || sortOrder.equals("")) {
                sortOrder = "DESC";
            }
            if (!(sortOrder.toUpperCase().equals("DESC") || sortOrder.toUpperCase().equals("ASC"))) {
                throw new Exception("Sort Order must be DESC or ASC");
            }
            List<JobExecutionStatusDto> jobs = runner_native_sql.getAllJobs(sortOrder);
            return new ResponseEntity(jobs, HttpStatus.OK);
        }
        catch (SQLException e) {
            return new ResponseEntity(e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
        catch (RuntimeException e) {
            return new ResponseEntity(e.getMessage(), HttpStatus.OK);
        }
        catch (Exception e) {
            return new ResponseEntity(e.getMessage(), HttpStatus.BAD_REQUEST);
        }
    }

    @RequestMapping(value = "/{jobId}", method = RequestMethod.GET)
    public ResponseEntity getJobById(@PathVariable String jobId) {
        try {
            JobExecutionStatusDto job = runner_native_sql.getJobById(jobId);
            return new ResponseEntity(job, HttpStatus.OK);
        }
        catch (SQLException e) {
            return new ResponseEntity(e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
        catch (RuntimeException e) {
            return new ResponseEntity(e.getMessage(), HttpStatus.OK);
        }
    }

}