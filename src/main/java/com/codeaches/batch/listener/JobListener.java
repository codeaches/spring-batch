package com.codeaches.batch.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.annotation.AfterJob;
import org.springframework.batch.core.annotation.BeforeJob;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
public class JobListener implements JobExecutionListener {

	private static final Logger log = LoggerFactory.getLogger(JobListener.class);

	private final JdbcTemplate jdbcTemplate;

	@Autowired
	public JobListener(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	@BeforeJob
	public void beforeJob(JobExecution jobExecution) {

		if (jobExecution.getStatus() == BatchStatus.STARTED) {

			jdbcTemplate.query("SELECT count(*) FROM people", (rs, row) -> rs.getInt(1))
					.forEach(count -> log.info("!!! JOB STARTED! Found <" + count + "> records in the database."));
		}
	}

	@AfterJob
	public void afterJob(JobExecution jobExecution) {

		if (jobExecution.getStatus() == BatchStatus.COMPLETED) {

			jdbcTemplate.query("SELECT count(*) FROM people", (rs, row) -> rs.getInt(1))
					.forEach(count -> log.info("!!! JOB COMPLETED! Found <" + count + "> records in the database."));
		}
	}
}
