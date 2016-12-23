package com.streamsets.pipeline.sdk;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.sql.DataSource;

import com.streamsets.pipeline.stage.origin.mysql.MysqlSource;
import com.streamsets.pipeline.stage.origin.mysql.MysqlSourceConfig;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class MysqlContext {
	Properties config;
	String gtid_offset_file;
	public MysqlContext(){
		config = new Properties();
		try {
			InputStream in = new FileInputStream(System.getProperty("user.dir")+"/conf/mysql.properties");
			config.load(in);
			in.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		mergeSystemArgs();
		gtid_offset_file=config.getProperty("binlog.offset_file", "binlog_gtid.offset");
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
			Properties args = System.getProperties();
			config.setProperty("jdbc.url", args.getProperty("jdbc.url"));
			config.setProperty("jdbc.user", args.getProperty("jdbc.user"));
			config.setProperty("jdbc.pwd", args.getProperty("jdbc.pwd"));
			
			if(args.getProperty("binlog.includeTables")!=null)
				config.setProperty("binlog.includeTables", args.getProperty("binlog.includeTables"));
			if(args.getProperty("binlog.ignoreTables")!=null)
				config.setProperty("binlog.ignoreTables", args.getProperty("binlog.ignoreTables"));
			
			config.setProperty("binlog.offset_file", jobid+config.getProperty("binlog.offset_file","binlog_gtid.offset"));
			/*
			for(Object key : args.keySet()){
				String topic = (String)key;
				if(topic.startsWith("topic.")){
					if(config.getProperty(topic)!=null)
					{
						config.put(topic+jobid, args.getProperty(topic));//已存在，更名为topic+jobid
						config.remove(topic);
					}else 
						config.put(topic, args.getProperty(topic));//新增的topic
				}
			}*/
		}
	}
	
	public String loadGtidfromFile(){
		return read(gtid_offset_file);
	}
	
	private static String read(String path){
		BufferedReader br=null;
		try {
			br = new BufferedReader(new FileReader(path));
			return br.readLine();
		}catch(FileNotFoundException ex){
			try {
				FileWriter resultFile = new FileWriter(path);
				resultFile.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		finally{
			if(br!=null)
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
		}
		return null;
	}
	

	protected void execute(DataSource ds, List<String> sql) throws SQLException {
	    try (Connection conn = ds.getConnection()) {
	      for (String s : sql) {
	        try (Statement stmt = conn.createStatement()) {
	          stmt.executeUpdate(s);
	          stmt.close();
	        }
	      }
	      conn.commit();
	    }
	  }
	
	protected HikariDataSource connect() {
	    HikariConfig hikariConfig = new HikariConfig();
	    hikariConfig.setJdbcUrl(config.getProperty("jdbc.url"));
	    hikariConfig.setUsername(config.getProperty("jdbc.user"));
	    hikariConfig.setPassword(config.getProperty("jdbc.pwd"));
	    hikariConfig.addDataSourceProperty("useSSL", false);
	    hikariConfig.setAutoCommit(false);
	    return new HikariDataSource(hikariConfig);
	  }
	 

	  public MysqlSource createMysqlSource() {
	    return new MysqlSource() {
	    	
	    	 private MysqlSourceConfig createConfig() {
	    		    MysqlSourceConfig cfg = new MysqlSourceConfig();
	    		    cfg.username = config.getProperty("jdbc.user");
	    		    cfg.password = config.getProperty("jdbc.pwd");
	    		    Matcher matcher = Pattern.compile("jdbc:mysql://(.*):(\\d+)/")
	    		        .matcher(config.getProperty("jdbc.url"));
	    		    matcher.find();
	    		    cfg.port = matcher.group(2);
	    		    cfg.hostname = matcher.group(1);
	    		    cfg.serverId = config.getProperty("binlog.serverId","999");
	    		    cfg.maxWaitTime = 1000;
	    		    cfg.maxBatchSize = Integer.parseInt(config.getProperty("binlog.batchSize","1000"));
	    		    cfg.connectTimeout = 5000;
	    		    cfg.initialOffset = loadGtidfromFile();
	    		    cfg.startFromBeginning = false;
	    		    
	    		    cfg.includeTables = config.getProperty("binlog.includeTables",null);
	    		    cfg.ignoreTables = config.getProperty("binlog.ignoreTables",null);
	    		    cfg.mergeEventMs = Integer.parseInt(config.getProperty("binlog.mergeEventMs","3000"));
	    		    return cfg;
	    		  }
	    	 
	    	 MysqlSourceConfig settings;
		      @Override
		      public MysqlSourceConfig getConfig() {
		    	  if(settings==null)
		    			settings = createConfig();
		    	  return settings;
		      }
	    };
	  }

}
