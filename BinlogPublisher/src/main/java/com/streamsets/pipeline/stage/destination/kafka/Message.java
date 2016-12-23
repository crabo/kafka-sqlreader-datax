package com.streamsets.pipeline.stage.destination.kafka;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import com.alibaba.fastjson.JSONObject;
import com.streamsets.pipeline.stage.origin.mysql.filters.Filter;
import com.streamsets.pipeline.stage.origin.mysql.filters.Filters;
import com.streamsets.pipeline.stage.origin.mysql.filters.IncludeTableFilter;

public class Message {
	private static final Logger LOG = LoggerFactory.getLogger(Message.class);
	
	Map<String,Filter> topicMap;
	String defaultTopic;
	String gtid_offset_file;
	Properties props;
	public Message() {
		this.props = new Properties();
		//new FileInputStream(path)
		try {
			InputStream in = new FileInputStream("conf/mysql.properties");
			props.load(in);
		} catch (IOException e) {
			e.printStackTrace();
		}
		mergeSystemArgs();
		gtid_offset_file=props.getProperty("binlog.offset_file", "binlog_gtid.offset");
		loadTopicSetting();
	}
	/**
	 * 如main传入了jobid参数，则遍历外部参数进行覆盖
jdbc.url = jdbc:mysql://106.14.25/yfxdb
jdbc.user= root
jdbc.pwd = ***

binlog.offset_file=binlog_gtid.offset
binlog.includeTables = yfxdb.%
binlog.ignoreTables  =


topic.customer = *
topic.trade = db1.customer%,db2,db3
topic.behavior = db5,db6
	 */
	void mergeSystemArgs(){
		String jobid=System.getProperty("job_id","");
		if(jobid.length()>0){//
			Properties config = props;
			Properties args = System.getProperties();
			/*
			config.put("jdbc.url", args.getProperty("jdbc.url"));
			config.put("jdbc.user", args.getProperty("jdbc.user"));
			config.put("jdbc.pwd", args.getProperty("jdbc.pwd"));
			
			if(args.getProperty("binlog.includeTables")!=null)
				config.put("binlog.includeTables", args.getProperty("binlog.includeTables"));
			if(args.getProperty("binlog.ignoreTables")!=null)
				config.put("binlog.ignoreTables", args.getProperty("binlog.ignoreTables"));
			*/
			config.setProperty("binlog.offset_file", jobid+config.getProperty("binlog.offset_file","binlog_gtid.offset"));
			
			for(Object key : args.keySet()){
				String topic = (String)key;
				
				if(topic.startsWith("topic.")){
					if(config.getProperty(topic)!=null)
					{
						config.setProperty(topic+jobid, args.getProperty(topic));//已存在，更名为topic+jobid
						config.remove(topic);
					}else 
						config.setProperty(topic, args.getProperty(topic));//新增的topic
				}
			}
			
		}
	}
	
	/**
==== file config.properties =====
topic.default_slot = %
topic.topic_slot_1 = db1.table1%,db2,db3
topic.topic_slot_2 = db%.table2%,db6
	 */
	private void loadTopicSetting(){
		topicMap = new LinkedHashMap<String,Filter>();
		int i=0;
		for(Entry<Object,Object> prop : props.entrySet()){
			String key = (String)prop.getKey();
			if(key.indexOf("topic.")==0){
				String topic=key.substring(6);
				String val = (String)prop.getValue();
				
				topicMap.put(
					topic, 
					createIncludeFilter(val)
				);
				i++;
				LOG.info("match tables '{}' to kafka topic '{}'",val,topic);
			}
		}
		LOG.info("==register to {}# kafka topics==",i);
	}
	public String getMessageId(JSONObject json){
		return json.getString("Database")+"."+json.getString("Table");
	}
	public String getTopic(JSONObject json){
		String db=json.getString("Database"),table=json.getString("Table");
		for(Entry<String,Filter> kv: topicMap.entrySet()){
			if(kv.getValue().apply(db, table)== Filter.Result.PASS)
				return kv.getKey();
		}
		
		return null;
	}
	
	private Filter createIncludeFilter(String includeTables) {
	    // if there are no include filters - pass
	    Filter filter = Filters.PASS;
	    if (includeTables != null && !includeTables.isEmpty()) {
	      String[] tables = includeTables.split(",");
	      if (tables.length > 0) {
	        // ignore all that is not explicitly included
	        filter = Filters.DISCARD;
	        for (String table : tables) {
	          if (!table.isEmpty()) {
	            filter = filter.or(new IncludeTableFilter(table));
	          }
	        }
	      }
	    }
	    return filter;
	  }
	
	public Integer getPartition(JSONObject json){
		return null;
	}
	public Long getTimestamp(JSONObject json){
		return json.getLongValue("Timestamp");
	}
	
	public void updateGtid(String lastSourceOffset){
		write(this.gtid_offset_file,lastSourceOffset);
	}
	
	static void write(String path,String val){
		BufferedWriter bw=null;
		try {
			bw = new BufferedWriter(new FileWriter(path));
			bw.write(val);
		} catch (IOException e) {
			e.printStackTrace();
		}
		finally{
			if(bw!=null)
				try {
					bw.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
		}
	}
}
