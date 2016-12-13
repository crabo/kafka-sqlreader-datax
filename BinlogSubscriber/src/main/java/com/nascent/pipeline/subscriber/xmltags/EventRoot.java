package com.nascent.pipeline.subscriber.xmltags;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;


@XmlRootElement(name="root")
public class EventRoot {
	private static final Logger LOGGER = LoggerFactory.getLogger(EventRoot.class);
	
	@XmlAttribute
	public String topics;
	
	@XmlElement(name = "event")
	public List<EventItem> events=new ArrayList<>();
	
	private List<EventItem> AnyDbEvents;
	private HashMap<String,List<EventItem>> MatchDbEvents;
	public void init(){
		AnyDbEvents = new ArrayList<>();
		MatchDbEvents=new HashMap<>();
		
		events.forEach(evt->{
			if("*".equals(evt.database))
				AnyDbEvents.add(evt);
			else
			{
				if(!MatchDbEvents.containsKey(evt.database))
					MatchDbEvents.put(evt.database, new ArrayList<>());
				
				MatchDbEvents.get(evt.database).add(evt);
			}
		});
	}
	
	/**
	 * binlog数据为 json["Data"]
	 * 返回结果在json中： json["Result"], json["Transformer"]
	 * @param database
	 * @param table
	 * @param type
	 * @param json
	 */
	public void route(String database,String table,String type,JSONObject json)
	{
		List<EventItem> li = MatchDbEvents.containsKey(database)?MatchDbEvents.get(database):AnyDbEvents;
		boolean isprocess=false;
		for(EventItem evt:li){
			if(table.equals(evt.table) && 
				("*".equals(evt.type) || type.equals(evt.type))){
				evt.process(json);
				isprocess=true;
			}
		}
		
		if(!isprocess){
			LOGGER.info("event '{}' on '{}.{}' is not subscribe",type,database,table);
		}
	}
	
	public static Map<String,Object> getData(JSONObject json){
		return (Map<String,Object>)json.get("Data");
	}
}
