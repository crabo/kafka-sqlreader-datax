package com.streamsets.pipeline.stage.destination.kafka;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.sdk.record.MapRecordImpl;
import com.streamsets.pipeline.stage.origin.mysql.MysqlSource;

public class KafkaTarget {//extends Thread 
	private static final Logger LOG = LoggerFactory.getLogger(KafkaTarget.class);
	private Producer<String, String> producer;
	private BlockingQueue<Record> queue;
	private Message kafkaMessage;
	
	public KafkaTarget() {
		//this.queue = new BlockingQueue<Record>(100);
		
		Properties props = new Properties();
		//new FileInputStream(path)
		try {
			InputStream in = new FileInputStream("conf/kafka.properties");
			
			props.load(in);
			in.close();
		} catch (IOException e) {
			LOG.warn("loading conf kafka.properties error",e);
		}
		
		this.producer = new KafkaProducer<String, String>(props);
		this.kafkaMessage = new Message();
	}
	
	public void send(Record rec) {
		queue.put(rec);
	}
	
	long tsBegin=System.currentTimeMillis();
	int count=0,i=0;
	public void send(List<Record> batch,String lastSourceOffset) {
		if(LOG.isInfoEnabled()){
			i++;
			count= count+batch.size();
		
			if(i>30){
		    	LOG.info("-- sending speed {} rows/sec, {} records --",count*1000/(System.currentTimeMillis()-tsBegin) ,count);
		    	
		    	i=0;count=0;
		    	tsBegin=System.currentTimeMillis();
		    }
		}
		send(batch.iterator());
		kafkaMessage.updateGtid(lastSourceOffset);
	}
	
	public void flush() {
		List<Record> Records = new ArrayList<>();
		
		Record Record = queue.poll();
		
		while(Record != null) {
			Records.add(Record);

			if(Records.size() > 100) {
				send(Records.iterator());
				
				Records.clear();
			}

			Record = queue.poll();
		}
		
		if(Records.size() > 0)
			send(Records.iterator());
	}
	
	private void send(Iterator<Record> Records) {
		try {
			sendUnsafe(Records);
		} catch(Exception e) {
			e.printStackTrace();

			try {
				if(this.producer!=null)
					this.producer.close();
				Thread.sleep(3000);
			} catch(InterruptedException interruptedException) {
				interruptedException.printStackTrace();
			}
	    	LOG.error("send to kafka failed!!",e);
		    
			System.exit(-1);
		}
	}
	
	private void sendUnsafe(Iterator<Record> records) {
		for(int tries = 0;; tries++) {
			try {
				while (records.hasNext()) {
					MapRecordImpl record = (MapRecordImpl)records.next();
					JSONObject json = new JSONObject();
					json.put("Type", record.getValue("Type"));
					json.put("Database", record.getValue("Database"));
					json.put("Table", record.getValue("Table"));
					json.put("Timestamp", record.getValue("Timestamp"));
					
					json.put("Data", (Map<String,Object>)record.getValue("Data"));
					
					ProducerRecord<String, String> msg = new ProducerRecord<String, String>(
							kafkaMessage.getTopic(json),
							kafkaMessage.getPartition(json),
							kafkaMessage.getTimestamp(json),
							kafkaMessage.getMessageId(json),
							json.toJSONString());
					producer.send(msg);
				}
				return;
			} catch(Exception e) {
				LOG.error("send to kafka failed!!",e);
				try {
					Thread.sleep(3000);
				} catch(InterruptedException interruptedException) {
					interruptedException.printStackTrace();
				}
				
				if(tries > 2)
					throw(e);
			}
		}
	}
	
	public void run() {
		while(true) {
			queue.waitIfNeccessary();
			
			flush();
		}
	}
	
	  public void destroy() {
	    if(producer!=null)
	    	producer.close();
	  }
}