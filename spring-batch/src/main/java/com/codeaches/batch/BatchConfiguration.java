package com.codeaches.batch;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

import com.codeaches.batch.listener.JobListener;
import com.codeaches.batch.listener.StepListener;
import com.codeaches.batch.pojo.Record;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

	@Autowired
	DataSource dataSource;

	@Autowired
	public JobBuilderFactory job;

	@Autowired
	public StepBuilderFactory step;

	@Bean
	public FlatFileItemReader<Record> reader() {

		return new FlatFileItemReaderBuilder<Record>()
				.name("recordItemReader")
				.resource(new ClassPathResource("records.csv"))
				.delimited()
				.names(new String[] { "firstName", "lastName" })
				.fieldSetMapper(new BeanWrapperFieldSetMapper<Record>() {
					{
						setTargetType(Record.class);
					}
				})
				.linesToSkip(3)
				.build();
	}

	@Bean
	public RecordProcessor processor() {
		return new RecordProcessor();
	}

	@Bean
	public JdbcBatchItemWriter<Record> writer() {

		return new JdbcBatchItemWriterBuilder<Record>()
				.itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
				.sql("INSERT INTO people (first_name, last_name) VALUES (:firstName, :lastName)")
				.dataSource(dataSource)
				.build();
	}

	@Bean
	public Job importRecordsJob(JobListener jobListener, Step etlStep) {

		return job.get("importRecordsJob")
				.incrementer(new RunIdIncrementer())
				.listener(jobListener)
				.flow(etlStep)
				.end()
				.build();
	}

	@Bean
	public Step etlRecordsStep(StepListener stepListener) {

		return step.get("etlRecordsStep")
				.<Record, Record>chunk(10)
				.reader(reader())
				.processor(processor())
				.writer(writer())
				.listener(stepListener)
				.faultTolerant()
				.skip(FlatFileParseException.class)
				.skipLimit(-1)
				.build();
	}
}
