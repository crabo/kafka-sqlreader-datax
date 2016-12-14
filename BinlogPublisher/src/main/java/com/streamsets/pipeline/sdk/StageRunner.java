package com.streamsets.pipeline.sdk;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.stage.destination.kafka.KafkaTarget;
import com.streamsets.pipeline.stage.origin.mysql.MysqlSource;

public class StageRunner  implements  Runnable {
	private static final Logger LOG = LoggerFactory.getLogger(StageRunner.class);
	StageContext context;
	MysqlSource source;
	KafkaTarget target;
	int batcSize=100;
	
	public void init(){
		context = new StageContext();
		
		source = new MysqlContext().createMysqlSource();
		source.init(null, context);
		batcSize=source.getConfig().maxBatchSize;
		
		target = new KafkaTarget();
	}
	

	
	@Override
	public void run() {
		this.init();
		try {
			this.run(source.getConfig().initialOffset);
		} catch (StageException e) {
			LOG.error("fetal error occured",e);
		} catch (InterruptedException e) {
			LOG.error("thread error occured",e);
		}
		
	}
	
	/**
	 * 
	 * @param startOffset格式  ${binlog-filename}:${binlog-position}
	 */
	public void run(String startOffset) throws StageException,InterruptedException{
		SingleLaneBatchMakerImpl batchMaker = new SingleLaneBatchMakerImpl("binlog_kafka");
		while(true){
		    String newOffset = source.produce(startOffset, this.batcSize, batchMaker);
		    List<Record> batch = batchMaker.getOutput();
		    if(batch.isEmpty()){
				Thread.sleep(250);
		    }else{
		    	target.send(batch, newOffset);
		    	batchMaker.clear();
		    }
		    
		    startOffset=newOffset;//move next!
	    }
	}
	
	public void runBatch(String lastOffset) throws StageException {
		SingleLaneBatchMakerImpl batchMaker = new SingleLaneBatchMakerImpl("binlog_kafka");
	    String newOffset = source.produce(lastOffset, this.batcSize, batchMaker);
	    target.send(batchMaker.getOutput(), newOffset);
	}

}
