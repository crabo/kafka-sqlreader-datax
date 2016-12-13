package com.nascent.pipeline.subscriber.xmltags;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;

import com.alibaba.fastjson.JSONObject;

public class EventItem {
	  @XmlAttribute
	  public String database;
	  @XmlAttribute
	  public String table;
	  @XmlAttribute
	  public String type;
	  
	  @XmlElement(name = "if")
	  public List<WhenIf> Ifs = new ArrayList<>();
	  
	  @XmlElement(name = "else")
	  public WhenElse Else;
	  
	  public void process(JSONObject json)
	  {
		  boolean isprocess=false;
		  for(WhenIf test : Ifs){
			  if(test.testAssert(EventRoot.getData(json))){
				  test.Transformer.transform(json);
				  isprocess=true;
			  }
		  }
		  if(!isprocess && Else!=null)
		  {
			  Else.Transformer.transform(json);
		  }
	  }
}
