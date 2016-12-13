package com.nascent.pipeline.subscriber.xmltags;

import java.util.Map;

import javax.xml.bind.annotation.XmlAttribute;

import com.alibaba.fastjson.JSONObject;
import com.nascent.pipeline.subscriber.ExpressionEvaluator;

public class TransBind {
	  @XmlAttribute
	  public String name;
	  @XmlAttribute
	  public String value;
	  
	  public Object getValue(Map<String,Object> data){
		  if(value.indexOf("#")<0){
			  return data.get(value);
		  }else{
			  return ExpressionEvaluator.getValue(value, data, null);
		  }
	  }
}
