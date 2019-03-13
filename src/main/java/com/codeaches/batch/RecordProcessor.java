package com.codeaches.batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.batch.item.ItemProcessor;

import com.codeaches.batch.pojo.Record;

public class RecordProcessor implements ItemProcessor<Record, Record> {

	private static final Logger log = LoggerFactory.getLogger(RecordProcessor.class);

	@Override
	public Record process(final Record record) throws Exception {

		final String firstName = record.getFirstName().toUpperCase();
		final String lastName = record.getLastName().toUpperCase();

		final Record transformedPerson = new Record(firstName, lastName);

		log.info("Converting (" + record + ") into (" + transformedPerson + ")");

		return transformedPerson;
	}
}
