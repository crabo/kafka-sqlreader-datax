package com.nascent.pipeline.subscriber.xmltags;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;

import com.alibaba.fastjson.JSONObject;
import com.nascent.pipeline.processor.mysql.MysqlProcessor;
import com.nascent.pipeline.subscriber.ExpressionEvaluator;

public class Transform {

	  @XmlAttribute
	  public String type;
	  @XmlAttribute
	  public String process;
	  
	  @XmlElement(name = "bind")
	  public List<TransBind> binds = new ArrayList<>();
	  @XmlElement(name = "map")
	  public List<TransMap> maps = new ArrayList<>();
	  
	  /**
	   * 将binlog结果映射到json["Result"]中
	   */
	  public void transform(JSONObject json){
		  Map<String,Object> data = EventRoot.getData(json);
		  for(TransBind bind : binds){
			  data.put(bind.name, bind.getValue(data));
		  }
		  
		  //执行取数
		  if(type!=null)
			  process(type,json);
		  
		  //从加工后的data中，提取map指定输出的变量名
		  if(!json.containsKey("Result") && maps.size()>0){
			  Map<String,Object> result = new HashMap<String,Object>(maps.size());
			  for(TransMap map : maps){
				  result.put(map.name, map.getValue(data));
			  }
			  json.put("Result", result);
		  }
	  }
	  
	  /**
	   * 数据生成后， 应重新写入json["data"]中，以便mapping输出
	   * @param processor
	   * @param json
	   */
	  //TODO: JAX如何动态创建不同的transformer?
	  protected void process(String processor,JSONObject json){
		  switch(processor)
		  {
			  case "map"://default type
				  //NOTHING TO DO
				  break;
			  case "sql":
				  json.put("Transformer",this.process);
				  break;
			  case "resultset":
				  getSqlProcessor().process(this.process, json);
				  break;
			  default:
				  ExpressionEvaluator.getValue(this.process, json, null);
				  break;
		  }
	  }
	  
	  MysqlProcessor sqlprocessor;
	  MysqlProcessor getSqlProcessor(){
		  if(sqlprocessor==null){
			  synchronized(this) {
				  if(sqlprocessor==null)
					  sqlprocessor=new MysqlProcessor();
			  }
		  }
		  return sqlprocessor;
	  }
}
