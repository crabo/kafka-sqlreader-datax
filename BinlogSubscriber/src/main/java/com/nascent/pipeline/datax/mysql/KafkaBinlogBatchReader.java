package com.nascent.pipeline.datax.mysql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.element.BoolColumn;
import com.alibaba.datax.common.element.BytesColumn;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.statistics.PerfRecord;
import com.alibaba.datax.common.statistics.PerfTrace;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSONObject;
import com.nascent.pipeline.datax.mysql.BatchExecutingInfo.ExecutingInfo;
import com.nascent.pipeline.processor.mysql.MysqlProcessor;
import com.nascent.pipeline.subscriber.KafkaBinlogConsumer;

public class KafkaBinlogBatchReader extends Reader {
	public static class Job extends Reader.Job {
		public static final String KEY_TOPIC="topics";
		private static int KAFKA_BATCH_SIZE;
		
		public static Map<String,ArrayBlockingQueue<ExecutingInfo>> ShareMsgBus;
		public static Map<String,KafkaBinlogConsumer> KafkaConsumers;
		public static Map<String,String> IgnoreTables;
        private static final Logger LOG = LoggerFactory
                .getLogger(Job.class);
        private Configuration originConfig = null;
		
        @Override
		public void init() {
			this.originConfig = this.getPluginJobConf();
			KAFKA_BATCH_SIZE=this.originConfig.getInt("kafkaBatchSize",100);
		}

        
        @Override
		public void prepare() {
        	IgnoreTables = new MysqlProcessor().IgnoreTables;
        	KafkaConsumers = new HashMap<>();
        	ShareMsgBus = new HashMap<>();
        }
        
        public static ArrayBlockingQueue<ExecutingInfo> connectKafka(String topic,int seq){
        	if(!KafkaConsumers.containsKey(topic)){
        		synchronized (KafkaConsumers){
        			if(!KafkaConsumers.containsKey(topic)){
	        			if(!ShareMsgBus.containsKey(topic)){//Msgbus connect to consumer
	        				ShareMsgBus.put(topic, new ArrayBlockingQueue<ExecutingInfo>(KAFKA_BATCH_SIZE));
	        			}
        				
        				LOG.info("connecting kafka consumer[{}] to topic '{}'",seq,topic);
        	        	KafkaBinlogConsumer consumer = KafkaBinlogConsumer.using(
        	        			seq,
        						topic.split(","), 
        						json->addTaskIfAbsent(topic,seq,json)
        					);
        				
        	        	KafkaConsumers.put(topic,consumer);
        	        	
        	        	//START reading!
        	        	consumer.runAsThread();
        			}
        		}
        	}
        	return ShareMsgBus.get(topic);
        }
        private static boolean addTaskIfAbsent(String topic,int seq,JSONObject json){
        	ArrayBlockingQueue<ExecutingInfo> queue = ShareMsgBus.get(topic);
        	
    		ExecutingInfo info = new ExecutingInfo(json.getString("Database"),json.getLong("Timestamp"));
    		if(!queue.contains(info))
    		{
    			try {
    				queue.put(info);//持续等待，直至队列数<100

    				if(LOG.isDebugEnabled())
    					LOG.debug("added kafka message {}@{}",json.getLong("Timestamp"),json.getString("Database"));
    				
    				KafkaConsumers.get(topic)
    					.tryCommit(json.getString("KafkaTopic"),json.getLong("KafkaOffset"));
    				return true;
    			} catch (InterruptedException e) {
    				LOG.error("addTaskIfAbsent Interrupted:{}",e);
    			}
    		}
    		return false;
    	}
        
        @Override
		public void post() {
		}

		@Override
		public void destroy() {
			for(KafkaBinlogConsumer comsumer: KafkaConsumers.values())
			{
				if(comsumer!=null)
					comsumer.shutdown();
			}
		}
		
		@Override
		public List<Configuration> split(int adviceNumber) {
			List<Configuration> readerSplitConfigs = new ArrayList<Configuration>();
			int partition = (adviceNumber/3)+1;
			for(int i=0;i<partition;i++){
				Configuration splitedConfig = this.originConfig.clone();
				readerSplitConfigs.add(splitedConfig);
			}
			return readerSplitConfigs;
		}
	}
	
	public static class Task extends Reader.Task {
		private static Logger LOG = LoggerFactory.getLogger(Task.class);

		private Configuration readerSliceConfig;

		private String username;
        private String password;
        private String jdbcUrl;
        private String querySql;
        
        private DataBaseType dataBaseType;
        private int taskGroupId = -1;
        private int taskId=-1;
        private int batch_interval;
        private int tsAdjust;//初次从mysql取得timestamp时，sql语句需要调节到更早的时间？
        private int fetchSize;
        private String tsStart;
        Map<String,Long> ExecutingTimestamps;
        SimpleDateFormat tsFormat;
        
        public Task() {
        	this(DataBaseType.MySql, -1, -1);
        	this.readerSliceConfig = Configuration.from("{}");
        }
        public Task(DataBaseType dataBaseType) {
            this(dataBaseType, -1, -1);
        }

        public Task(DataBaseType dataBaseType,int taskGropuId, int taskId) {
            this.dataBaseType = DataBaseType.MySql;
            this.taskGroupId = taskGropuId;
            this.taskId = taskId;
        }
        
		@Override
		public void init() {
			this.readerSliceConfig = this.getPluginJobConf();
			if(this.readerSliceConfig.getString(Job.KEY_TOPIC)==null){
				throw new RuntimeException("'topics' field is required in reader config ");
			}
			
			this.username = readerSliceConfig.getString("username");
            this.password = readerSliceConfig.getString("password");
            this.jdbcUrl = readerSliceConfig.getString("jdbcUrl");
            this.querySql= readerSliceConfig.getString("querySql");
            this.fetchSize = readerSliceConfig.getInt("fetchSize",1000);
            this.tsStart = readerSliceConfig.getString("tsStart");
            
            this.tsAdjust = readerSliceConfig.getInt("tsAdjust",0)*1000;
            this.batch_interval = readerSliceConfig.getInt("batch_interval_sec",10)*1000;
            
            if(this.querySql==null)
            	throw new RuntimeException("'querySql' field is required in reader config ");
            
            if(this.querySql.indexOf("$ts_start")<0)
            	throw new RuntimeException("string '$ts_start' is required in reader field 'querySql' ");
            //安全性： 防止测试语句时，数据进入其他客户库
            if(this.querySql==null||this.querySql.indexOf("$database")<0)
            	throw new RuntimeException("[$database] is required in 'querySql' config ");
            this.taskId=super.getTaskId();
		}

		
		@Override
		public void prepare() {
			ExecutingTimestamps=new HashMap<>();
			tsFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		}

		@Override
		public void post() {

		}

		@Override
		public void destroy() {
			
		}

		RecordSender recordSender;
		ArrayBlockingQueue<ExecutingInfo> queue;
		@Override
		public void startRead(RecordSender recordSender) {
			this.recordSender = recordSender;
			
			String topic = this.readerSliceConfig.getString(Job.KEY_TOPIC);
			this.queue = Job.connectKafka(topic,taskId);
			
			LOG.info("task[{}] start consuming kafka topic '{}'",taskId,topic);
			
			while(true){
        		try{
        			daemonCheck();
            	} catch (InterruptedException e) {
            		LOG.error("task[{}] daemonCheck error:{}",taskId,e);
    			}
        	}
		}
		/**
		 * 未达到10s等待时间的数据不允许出队列
		 */
		private void daemonCheck() throws InterruptedException{
			if(queue.isEmpty()){
				Thread.sleep(2000);
			}
			//统一个select 至少间隔时间，如10s
			else if(System.currentTimeMillis() - queue.peek().addedTime>this.batch_interval){
				//仅当前startRead完成， 才会读取队列下一个execInfo
				ExecutingInfo e = queue.poll();
				this.startRead(e.database,e.timestamp);
			}else{
				Thread.sleep(300);
			}
		}
		
		/**
		 * 仅当前startRead完成， 才会读取队列下一个execInfo
		 */
		public void startRead(String database,long timestamp){
			if(Job.IgnoreTables.containsKey(database)){
				return;
			}
			String sql = this.getSql(database, timestamp);
			executeSql(sql,database,null,this.recordSender,super.getTaskPluginCollector(),this.fetchSize);
			
			if(LOG.isTraceEnabled())
				LOG.trace("task[{}] finish process message {}@{}",taskId,timestamp,database);
		}
		String getSql(String database,long timestamp){
			if(!ExecutingTimestamps.containsKey(database))//首次执行，开始时间为binlog时间戳
			{
				if(this.tsStart!=null){
					try {
						ExecutingTimestamps.put(database, tsFormat.parse(tsStart).getTime());
					} catch (ParseException e) {
						LOG.warn("wrong 'tsStart' format: {}",e);
						ExecutingTimestamps.put(database, timestamp);
					}
				}else
				    ExecutingTimestamps.put(database, timestamp);
			}
			
			String sql;
			long tsEnd = System.currentTimeMillis()-this.tsAdjust;
			synchronized (ExecutingTimestamps){
				sql = this.querySql
					.replace("$database", database)
					.replace("$ts_start", new Timestamp(ExecutingTimestamps.get(database)).toString() )
					.replace("$ts_end", new Timestamp( tsEnd).toString() );
				
				ExecutingTimestamps.put(database, tsEnd);//下次执行时，开始时间从当前tsNow开始计算
			}
			
			if(LOG.isDebugEnabled())
				LOG.debug("task[{}] reading sql ts_end='{}' {}@{}",taskId,new Timestamp(tsEnd),timestamp,database);
			return sql;
		}
		
		/**
		 * 要一条SQL从服务器读取大量数据，不发生JVM OOM，可以采用以下方法之一：

		1、当statement设置以下属性时，采用的是流数据接收方式，每次只从服务器接收部份数据，直到所有数据处理完毕。
          stmt.setResultSetType(ResultSet.TYPE_FORWARD_ONLY);
          stmt.setFetchSize(Integer.MIN_VALUE); 
		2、调用statement的enableStreamingResults方法，实际上enableStreamingResults方法内部封装的就是第1种方式。
		3、设置连接属性useCursorFetch=true (5.0版驱动开始支持)，TYPE_FORWARD_ONLY+fetch size参数，表示采用服务器端游标，每次从服务器取fetch_size条数据。
			stmt.setResultSetType(ResultSet.TYPE_FORWARD_ONLY);
            stmt.setFetchSize(1000);//任意>0 的fetch大小
                               如：jdbc:mysql://192.168.1.252:3306/testdb?charsetEncoding=utf8&useCursorFetch=true
		 */
		private void executeSql(String querySql,String database,String mandatoryEncoding,
                RecordSender recordSender,
                TaskPluginCollector taskPluginCollector, int fetchSize) {
		
			PerfTrace.getInstance().addTaskDetails(taskId, database);
			if(LOG.isTraceEnabled())
				LOG.trace("task[{}] begin to read record by Sql: [{}\n] {}.",taskId,querySql, database);
			PerfRecord queryPerfRecord = new PerfRecord(taskGroupId,taskId, PerfRecord.PHASE.SQL_QUERY);
			queryPerfRecord.start();
			
			Connection conn = DBUtil.getConnection(this.dataBaseType, jdbcUrl,
			      username, password);
			
			// session config .etc related
			//DBUtil.dealWithSessionConfig(conn, readerSliceConfig,
			//      this.dataBaseType, basicMsg);
			
			int columnNumber = 0;
			ResultSet rs = null;
			try {
			  rs = DBUtil.query(conn, querySql, fetchSize);
			  queryPerfRecord.end();
			
			  ResultSetMetaData metaData = rs.getMetaData();
			  columnNumber = metaData.getColumnCount();
			
			  //这个统计干净的result_Next时间
			  PerfRecord allResultPerfRecord = new PerfRecord(taskGroupId, taskId, PerfRecord.PHASE.RESULT_NEXT_ALL);
			  allResultPerfRecord.start();
			
			  long rsNextUsedTime = 0;
			  long lastTime = System.nanoTime();
			  while (rs.next()) {
			      rsNextUsedTime += (System.nanoTime() - lastTime);
			      this.transportOneRecord(recordSender, rs,
			              metaData, columnNumber, database, taskPluginCollector);
			      lastTime = System.nanoTime();
			  }
			  
			  allResultPerfRecord.end(rsNextUsedTime);
			
			}catch (Exception e) {
			  throw DataXException
              	.asDataXException(GenericErrorCode.ERROR,
              			"readRecord error: "+database,e);
			} finally {
				try {
					if(rs!=null)
						rs.close();
				} catch (SQLException e) {
					LOG.warn("close resultset error: {}",e);
				}
			  DBUtil.closeDBResources(null, conn);
			}
		}
		
		protected Record transportOneRecord(RecordSender recordSender, ResultSet rs, 
                ResultSetMetaData metaData, int columnNumber, String database, 
                TaskPluginCollector taskPluginCollector) {
            Record record = buildRecord(recordSender,rs,metaData,columnNumber,null,taskPluginCollector); 
            //by crabo: add $database at last column
            record.addColumn(new StringColumn(database));
            recordSender.sendToWriter(record);
            return record;
        }
        protected Record buildRecord(RecordSender recordSender,ResultSet rs, ResultSetMetaData metaData, int columnNumber, String mandatoryEncoding,
        		TaskPluginCollector taskPluginCollector) {
        	Record record = recordSender.createRecord();
            try {
                for (int i = 1; i <= columnNumber; i++) {
                    switch (metaData.getColumnType(i)) {

                    case Types.CHAR:
                    case Types.NCHAR:
                    case Types.VARCHAR:
                    case Types.LONGVARCHAR:
                    case Types.NVARCHAR:
                    case Types.LONGNVARCHAR:
                        String rawData;
                        if(StringUtils.isBlank(mandatoryEncoding)){
                            rawData = rs.getString(i);
                        }else{
                            rawData = new String((rs.getBytes(i) == null ? new byte[0] : 
                                rs.getBytes(i)), mandatoryEncoding);
                        }
                        record.addColumn(new StringColumn(rawData));
                        break;

                    case Types.CLOB:
                    case Types.NCLOB:
                        record.addColumn(new StringColumn(rs.getString(i)));
                        break;

                    case Types.SMALLINT:
                    case Types.TINYINT:
                    case Types.INTEGER:
                    case Types.BIGINT:
                        record.addColumn(new LongColumn(rs.getString(i)));
                        break;

                    case Types.NUMERIC:
                    case Types.DECIMAL:
                        record.addColumn(new DoubleColumn(rs.getString(i)));
                        break;

                    case Types.FLOAT:
                    case Types.REAL:
                    case Types.DOUBLE:
                        record.addColumn(new DoubleColumn(rs.getString(i)));
                        break;

                    case Types.TIME:
                        record.addColumn(new DateColumn(rs.getTime(i)));
                        break;

                    // for mysql bug, see http://bugs.mysql.com/bug.php?id=35115
                    case Types.DATE:
                        if (metaData.getColumnTypeName(i).equalsIgnoreCase("year")) {
                            record.addColumn(new LongColumn(rs.getInt(i)));
                        } else {
                            record.addColumn(new DateColumn(rs.getDate(i)));
                        }
                        break;

                    case Types.TIMESTAMP:
                        record.addColumn(new DateColumn(rs.getTimestamp(i)));
                        break;

                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.BLOB:
                    case Types.LONGVARBINARY:
                        record.addColumn(new BytesColumn(rs.getBytes(i)));
                        break;

                    // warn: bit(1) -> Types.BIT 可使用BoolColumn
                    // warn: bit(>1) -> Types.VARBINARY 可使用BytesColumn
                    case Types.BOOLEAN:
                    case Types.BIT:
                        record.addColumn(new BoolColumn(rs.getBoolean(i)));
                        break;

                    case Types.NULL:
                        String stringData = null;
                        if(rs.getObject(i) != null) {
                            stringData = rs.getObject(i).toString();
                        }
                        record.addColumn(new StringColumn(stringData));
                        break;

                    default:
                        throw DataXException
                                .asDataXException(
                                		GenericErrorCode.ERROR,
                                        String.format(
                                                "您的配置文件中的列配置信息有误. 因为DataX 不支持数据库读取这种字段类型. 字段名:[%s], 字段名称:[%s], 字段Java类型:[%s]. 请尝试使用数据库函数将其转换datax支持的类型 或者不同步该字段 .",
                                                metaData.getColumnName(i),
                                                metaData.getColumnType(i),
                                                metaData.getColumnClassName(i)));
                    }
                }
            } catch (Exception e) {
                taskPluginCollector.collectDirtyRecord(record, e);
                if (e instanceof DataXException) {
                    throw (DataXException) e;
                }
            }
            return record;
        }

	}
}
