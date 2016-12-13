package com.streamsets.pipeline.stage.destination.kafka;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import com.alibaba.fastjson.JSONObject;
import com.streamsets.pipeline.api.Record;

public class Message {
	//map<database,topic> 
	Map<String,String> topicMap;
	String defaultTopic;
	String gtid_offset_file;
	Properties props;
	public Message() {
		this.props = new Properties();
		//new FileInputStream(path)
		InputStream in = getClass().getResourceAsStream("/mysql.properties");
		try {
			props.load(in);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		gtid_offset_file=props.getProperty("binlog.offset_file", "binlog_gtid.offset");
		loadTopicSetting();
	}
	/**
==== file config.properties =====
topic.default_slot = *
topic.topic_slot_1 = db1,db2,db3
topic.topic_slot_2 = db5,db6
	 */
	void loadTopicSetting(){
		topicMap = new HashMap<String,String>();
		for(Entry<Object,Object> prop : props.entrySet()){
			String key = (String)prop.getKey();
			if(key.indexOf("topic.")==0){
				String[] vals = ((String)prop.getValue()).split(",");
				for(String val : vals){
					if(defaultTopic==null && "*".equals(val))
						defaultTopic=key.substring(6);
					else//map<database , topic>
						topicMap.put(val, key.substring(6));
				}
			}
		}
	}
	public String getMessageId(JSONObject json){
		return json.getString("Database")+"."+json.getString("Table");
	}
	public String getTopic(JSONObject json){
		String t = topicMap.get(json.getString("Database"));
		return t!=null?t:defaultTopic;
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
