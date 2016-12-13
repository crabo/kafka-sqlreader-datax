package com.nascent.pipeline.processor.mysql;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.text.StrSubstitutor;
import com.alibaba.fastjson.JSONObject;
import com.nascent.pipeline.subscriber.xmltags.EventRoot;
import com.nascent.pipeline.subscriber.xmltags.TransMap;

public class MysqlProcessor {
	Properties config;
	public MysqlProcessor(){
		config = new Properties();
		try {
			InputStream in = new FileInputStream(System.getProperty("user.dir")+"/conf/mysql.properties");
			config.load(in);
			in.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		//this.ds = connect();
	}
	public static String getBoundSql(String sql, JSONObject json){
		if(sql.indexOf("${Database}")<0)
			throw new RuntimeException("Sql without specified '${Database}' is not allowed! ");
		StrSubstitutor binder = new StrSubstitutor(EventRoot.getData(json));
		return binder.replace(sql);
	}
	
	/**
	 * 结果集完全由sql决定，不再进行mapping
	 * 输出ArrayList of Map
	 * @param sql
	 * @param json
	 */
	public void process(String sql, JSONObject json){
		List<Map<String,Object>> li = execute(getBoundSql(sql,json));
		json.put("Result",li);
	}
	
	protected List<Map<String,Object>> execute(String sql){
		List<Map<String,Object>> li = new ArrayList<>();
		
	    try (Connection conn = null){//ds.getConnection()) {
	        try (Statement stmt = conn.createStatement()) {
	          ResultSet rs = stmt.executeQuery(sql);
	          
	          ResultSetMetaData metaData = rs.getMetaData();
	          int columnNumber = metaData.getColumnCount();
	          
	          while(rs.next()){
		          Map<String,Object> record=new HashMap<>(columnNumber);
		          ResultSetReadProxy.transportOneRecord(record, rs, metaData, columnNumber, null);
		          
		          li.add(record);
	          }
	          
	          stmt.close();
	      }
	      conn.commit();
	    }catch(SQLException ex){
	    	
	    }
	    return li;
	  }
	/*
	HikariDataSource ds;
	protected HikariDataSource connect() {
	    HikariConfig hikariConfig = new HikariConfig();
	    hikariConfig.setJdbcUrl(config.getProperty("jdbc.url"));
	    hikariConfig.setUsername(config.getProperty("jdbc.user"));
	    hikariConfig.setPassword(config.getProperty("jdbc.pwd"));
	    hikariConfig.addDataSourceProperty("useSSL", false);
	    hikariConfig.setAutoCommit(false);
	    return new HikariDataSource(hikariConfig);
	}*/
}
