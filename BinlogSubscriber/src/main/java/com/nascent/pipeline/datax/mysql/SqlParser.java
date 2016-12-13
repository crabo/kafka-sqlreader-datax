package com.nascent.pipeline.datax.mysql;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.datax.common.util.Configuration;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Timestamp;;

public class SqlParser {
	public static void init(){
		tsFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		ExecutingTimes=new HashMap<>();
		
		Configuration cfg=null;
		try {
			byte[] encoded = Files.readAllBytes(
					Paths.get(System.getProperty("user.dir"),"conf/sql_es.json"));
			cfg = Configuration.from(new String(encoded,"utf-8"));
		} catch (Exception e) {
			throw new RuntimeException("无效的配置文件'conf/sql_es.jon'");
		}
		
		List<Configuration> tasks = cfg.getListConfiguration("");
		ConfigSqls =new HashMap<>(tasks.size());
		for(Configuration t:tasks){
			ConfigSqls.put(t.getString("taskId"), t.getString("reader.querySql"));
		}
	}
	static DateFormat tsFormat; 
	static Map<String,Long> ExecutingTimes;
	static Map<String,String> ConfigSqls;
	public static String getSql(String sqlKey,String database,long timestamp){
		String key = database+sqlKey;
		if(!ExecutingTimes.containsKey(key))//首次执行，开始时间为binlog时间戳
			ExecutingTimes.put(key, timestamp-1000);//adjust
		
		String sql = getConfigSql(sqlKey);
		
		long tsNow=System.currentTimeMillis();
		synchronized (ExecutingTimes){
			sql = sql.replace("$database", database)
				.replace("$ts_start", tsFormat.format(new Timestamp( ExecutingTimes.get(key))) )
				.replace("$ts_end", tsFormat.format(new Timestamp( tsNow)) );
			
			ExecutingTimes.put(key, tsNow);//下次执行时，开始时间从当前tsNow开始计算
		}
		return sql;
	}
	private static String getConfigSql(String sqlKey)
	{
		return ConfigSqls.get(sqlKey);
	}
}
