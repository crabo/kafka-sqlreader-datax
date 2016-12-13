package com.streamsets.pipeline.sdk;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.EventRecord;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage.ConfigIssue;
import com.streamsets.pipeline.api.Stage.Info;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.ext.ContextExtensions;
import com.streamsets.pipeline.api.ext.RecordReader;
import com.streamsets.pipeline.api.ext.RecordWriter;
import com.streamsets.pipeline.api.ext.Sampler;
import com.streamsets.pipeline.sdk.record.EventRecordImpl;
import com.streamsets.pipeline.sdk.record.HeaderImpl;
import com.streamsets.pipeline.sdk.record.RecordImpl;
import com.streamsets.pipeline.stage.origin.mysql.MysqlSource;

public class StageContext implements Source.Context, Target.Context, Processor.Context, ContextExtensions {
	private static final Logger LOG = LoggerFactory.getLogger(StageContext.class);
	private final String instanceName = "binlog_kafka";
	
	@Override
	public ExecutionMode getExecutionMode() {
		return ExecutionMode.STANDALONE;
	}

	@Override
	public long getPipelineMaxMemory() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean isPreview() {
		return false;
	}

	@Override
	public ConfigIssue createConfigIssue(String configGroup, String configName, ErrorCode errorCode, Object... args) {
		return null;
	}

	@Override
	public List<Info> getPipelineInfo() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MetricRegistry getMetrics() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Timer createTimer(String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Timer getTimer(String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Meter createMeter(String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Meter getMeter(String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Counter createCounter(String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Counter getCounter(String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> Gauge<T> createGauge(String name, Gauge<T> gauge) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> Gauge<T> getGauge(String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void reportError(Exception exception) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void reportError(String errorMessage) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void reportError(ErrorCode errorCode, Object... args) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public OnRecordError getOnErrorRecord() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void toError(Record record, Exception exception) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void toError(Record record, String errorMessage) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void toError(Record record, ErrorCode errorCode, Object... args) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Record createRecord(String recordSourceId) {
		return new RecordImpl(instanceName, recordSourceId, null, null);
	}

	@Override
	public Record createRecord(String recordSourceId, byte[] raw, String rawMime) {
		return new RecordImpl(instanceName, recordSourceId, raw, rawMime);
	}

	@Override
	public long getLastBatchTime() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getResourcesDirectory() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isStopped() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public EventRecord createEventRecord(String type, int version) {
		return new EventRecordImpl(type, version, instanceName, "recordSourceId", null, null);
	}

	@Override
	public void toEvent(EventRecord record) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void parseEL(String el) throws ELEvalException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public ELVars createELVars() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ELEval createELEval(String configName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ELEval createELEval(String configName, Class<?>... elDefClasses) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RecordReader createRecordReader(InputStream inputStream, long initialPosition, int maxObjectLen)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RecordWriter createRecordWriter(OutputStream outputStream) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void notify(List<String> addresses, String subject, String body) throws StageException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Sampler getSampler() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Record createRecord(Record originatorRecord) {
		Preconditions.checkNotNull(originatorRecord, "originatorRecord cannot be null");
	    RecordImpl record = new RecordImpl(instanceName, originatorRecord, null, null);
	    HeaderImpl header = record.getHeader();
	    header.setStagesPath("");
	    return record;
	}

	@Override
	public Record createRecord(Record originatorRecord, String sourceIdPostfix) {
		Preconditions.checkNotNull(originatorRecord, "originatorRecord cannot be null");
	    RecordImpl record = new RecordImpl(instanceName, originatorRecord, null, null);
	    HeaderImpl header = record.getHeader();
	    header.setSourceId(header.getSourceId() + "_" + sourceIdPostfix);
	    header.setStagesPath("");
	    return record;
	}

	@Override
	public Record createRecord(Record originatorRecord, byte[] raw, String rawMime) {
		return new RecordImpl(instanceName, originatorRecord, raw, rawMime);
	}

	@Override
	public Record cloneRecord(Record record) {
		 RecordImpl clonedRecord = ((RecordImpl) record).clone();
	    HeaderImpl header = clonedRecord.getHeader();
	    header.setStagesPath("");
	    return clonedRecord;
	}

	@Override
	public Record cloneRecord(Record record, String sourceIdPostfix) {
		 RecordImpl clonedRecord = ((RecordImpl) record).clone();
		    HeaderImpl header = clonedRecord.getHeader();
		    header.setSourceId(header.getSourceId() + "_" + sourceIdPostfix);
		    header.setStagesPath("");
		    return clonedRecord;
	}

	@Override
	public List<String> getOutputLanes() {
		// TODO Auto-generated method stub
		return null;
	}

}
