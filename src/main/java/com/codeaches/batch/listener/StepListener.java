package com.codeaches.batch.listener;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.annotation.AfterChunk;
import org.springframework.batch.core.annotation.AfterChunkError;
import org.springframework.batch.core.annotation.AfterProcess;
import org.springframework.batch.core.annotation.AfterRead;
import org.springframework.batch.core.annotation.AfterWrite;
import org.springframework.batch.core.annotation.BeforeChunk;
import org.springframework.batch.core.annotation.BeforeProcess;
import org.springframework.batch.core.annotation.BeforeRead;
import org.springframework.batch.core.annotation.BeforeWrite;
import org.springframework.batch.core.annotation.OnProcessError;
import org.springframework.batch.core.annotation.OnReadError;
import org.springframework.batch.core.annotation.OnSkipInProcess;
import org.springframework.batch.core.annotation.OnSkipInRead;
import org.springframework.batch.core.annotation.OnSkipInWrite;
import org.springframework.batch.core.annotation.OnWriteError;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.stereotype.Component;

import com.codeaches.batch.pojo.Record;

@Component
public class StepListener {

	private static final Logger log = LoggerFactory.getLogger(StepListener.class);

	@BeforeWrite
	public void beforeWrite(List<? extends Record> items) {
		log.info("About to write " + items.size() + " records to db");
	}

	@AfterWrite
	public void afterWrite(List<? extends Record> items) {
		log.info("Wrote " + items.size() + " records to db");
	}

	@OnWriteError
	public void onWriteError(Exception e, List<? extends Record> items) {
		log.error("Unable to write " + items.size() + " records to db", e);
	}

	@BeforeChunk
	public void before(ChunkContext context) {
		log.info("Processing started on this chunk? <" + context.isComplete() + ">");
	}

	@AfterChunk
	public void after(ChunkContext context) {
		log.info("Processing completed on this chunk? <" + context.isComplete() + ">");
	}

	@AfterChunkError
	public void afterChunkError(ChunkContext context) {
		log.info("Processing failed for this chunk. Process complete status is <" + context.isComplete() + ">");
	}

	@BeforeRead
	public void beforeRead() {
		log.info("About to read a record from the file");
	}

	@AfterRead
	public void afterRead(Record item) {
		log.info("Completed reading " + item + " from the file");
	}

	@OnReadError
	public void onReadError(Exception e) {
		log.info("Unable to read a record from the file", e);
	}

	@BeforeProcess
	public void beforeProcess(Record item) {
		log.info("About to process " + item);
	}

	@AfterProcess
	public void afterProcess(Record item, Record itemNew) {
		log.info("Completed processing " + item + ". The processed record is " + itemNew);
	}

	@OnProcessError
	public void onProcessError(Record item, Exception e) {
		log.error("Unable to process " + item, e);
	}

	@OnSkipInRead
	public void onSkipInRead(Exception e) {
		log.error("Skip in read error", e);
	}

	@OnSkipInWrite
	public void onSkipInWrite(Record item, Exception e) {
		log.error("Skip in write error for item " + item, e);
	}

	@OnSkipInProcess
	public void onSkipInProcess(Record item, Exception e) {
		log.error("Skip in process error for item " + item, e);
	}
}
