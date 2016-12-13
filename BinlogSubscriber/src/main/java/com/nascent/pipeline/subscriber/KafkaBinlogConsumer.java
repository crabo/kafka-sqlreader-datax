package com.nascent.pipeline.subscriber;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import com.alibaba.fastjson.JSONObject;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaBinlogConsumer implements Runnable{
	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaBinlogConsumer.class);
	private final AtomicBoolean closed = new AtomicBoolean(false);
	private final long timeout;
	private final Collection<String> topics;
	
	private final Consumer<JSONObject> eventsConsumer;
	
	 public static KafkaBinlogConsumer using(String[] topics,Consumer<JSONObject> channel) {
		 return new KafkaBinlogConsumer(1000, Arrays.asList(topics),channel);
    }

	
	KafkaConsumer<String,String> consumer;
	private KafkaBinlogConsumer(long timeout,Collection<String> topics,Consumer<JSONObject> channel) {
		Properties props = new Properties();
		try {
			InputStream in = new FileInputStream(System.getProperty("user.dir")+"/conf/kafka.properties");
			
			props.load(in);
			in.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		this.consumer = new KafkaConsumer<>(props);
		this.timeout = timeout;
		this.topics = topics;
		this.eventsConsumer = channel;
	}
	@Override
	public void run() {
		try {
			consumer.subscribe(topics);
			CommitedTimeStore = new HashMap<>();
			LOGGER.info("subscribe to kafka ready ");
			
            while (!closed.get()) {
            	consumer.poll(timeout)
            		.forEach(this::processRecord);
            }
        } catch (WakeupException e) {
            if (!closed.get()) throw e;
        } finally {
        	consumer.close();
        }
    }

    private void processRecord(ConsumerRecord<String, String> record) {
        try {
        	if(LOGGER.isTraceEnabled())
        		LOGGER.trace("processRecord: topic='{}',ts='{}',partition='{}',offset='{}'",record.topic(),record.timestamp(),record.partition(),record.offset());
        	
        	JSONObject json = JSONObject.parseObject(record.value());
        	json.put("KafkaTopic", String.format("%s/%s", record.topic(),record.partition()));
        	json.put("KafkaOffset", record.offset());
        	
    		//Thread.sleep(1900); delay msg throughput for test purpose
        	eventsConsumer.accept(json);
        } catch (Exception e) {
            LOGGER.warn("Exception processing message 'offset={}': {}",record.offset(), e);
        }
    }

    public void shutdown() {
        closed.set(true);
        consumer.wakeup();
    }
    
    /**
     * 合并commit请求， 每60秒提交一次offset
     * @param topic
     * @param offset
     */
    public void tryCommit(String topic,long offset) {
    	if(!CommitedTimeStore.containsKey(topic)//同一topic仅60s才commit一次
    			|| System.currentTimeMillis()-CommitedTimeStore.get(topic)>60000)
    	{
    		CommitedTimeStore.put(topic, System.currentTimeMillis());
    	
    		String[] metas = topic.split("/");
	    	Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>(1);
	        offsets.put(
	        		new TopicPartition(metas[0], Integer.valueOf(metas[1])), 
	        		new OffsetAndMetadata(offset + 1));
	        consumer.commitSync(offsets);
	        
	        LOGGER.debug("commiting kafka offset {}@{}",topic,offset);
    	}
    }
    Map<String,Long> CommitedTimeStore;
}
