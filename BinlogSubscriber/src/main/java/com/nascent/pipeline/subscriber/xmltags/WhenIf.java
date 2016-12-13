package com.nascent.pipeline.subscriber.xmltags;

import java.util.Map;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;

import com.nascent.pipeline.subscriber.ExpressionEvaluator;

public class WhenIf {
	  @XmlAttribute
	  public String test;
	  
	  @XmlElement(name = "transform")
	  public Transform Transformer;
	  
	  public boolean testAssert(Map<String,Object> data){
		  if(this.test==null || this.test.isEmpty()){//未定义test条件
			    return true;
		  }else{
			  return (Boolean)ExpressionEvaluator.getValue(this.test, 
					  data,null);
		  }
	  }
}
