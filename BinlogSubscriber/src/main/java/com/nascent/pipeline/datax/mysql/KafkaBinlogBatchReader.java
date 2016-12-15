package com.nascent.pipeline.datax.mysql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import com.nascent.pipeline.processor.mysql.MysqlProcessor;
import com.nascent.pipeline.subscriber.KafkaBinlogConsumer;

public class KafkaBinlogBatchReader extends Reader {
	public static class Job extends Reader.Job {
		public static final String KEY_TOPIC="topics";
		
		public static Map<String,String> IgnoreTables;
        private static final Logger LOG = LoggerFactory
                .getLogger(Job.class);
        private Configuration originConfig = null;
		
        @Override
		public void init() {
			this.originConfig = this.getPluginJobConf();
		}

        
        @Override
		public void prepare() {
        	IgnoreTables = new MysqlProcessor().IgnoreTables;
        }
        @Override
		public void post() {
		}

		@Override
		public void destroy() {
			
		}
		
		@Override
		public List<Configuration> split(int adviceNumber) {
			List<Configuration> readerSplitConfigs = new ArrayList<Configuration>();
			String topics = this.originConfig.getString(KEY_TOPIC,"");
			String[] patitions = topics.split(";");
			for (int i=0;i<patitions.length;i++) {//按照channel*partition建立足够多的kafka连接
				for (int k=0;k<adviceNumber;k++)
				{
					Configuration splitedConfig = this.originConfig.clone();
					splitedConfig.set(KEY_TOPIC, patitions[i]);
					
					readerSplitConfigs.add(splitedConfig);
				}
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
        private int kafkaBatchSize;
        private String tsStart;
        KafkaBinlogConsumer comsumer;
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
            this.kafkaBatchSize = readerSliceConfig.getInt("kafkaBatchSize",50);
            this.fetchSize = readerSliceConfig.getInt("fetchSize",1000);
            this.tsAdjust = readerSliceConfig.getInt("tsAdjust",60000);
            this.tsStart = readerSliceConfig.getString("tsStart");
            
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
			batcher=new BatchExecutingInfo(this,batch_interval,kafkaBatchSize);
			ExecutingTimestamps=new HashMap<>();
			tsFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		}

		@Override
		public void post() {

		}

		@Override
		public void destroy() {
			if(comsumer!=null)
				comsumer.shutdown();
		}

		BatchExecutingInfo batcher;
		RecordSender recordSender;
		@Override
		public void startRead(RecordSender recordSender) {
			this.recordSender = recordSender;
			
			this.comsumer = KafkaBinlogConsumer.using(
					this.getTaskId(),
					this.readerSliceConfig.getString(Job.KEY_TOPIC).split(","), 
				json->{
					//kafka消息将排队等待进入blokingqueue； 并等待10s才执行,已存在的请求不再重复执行
					if(batcher.addTaskIfAbsent(
							json.getString("Database"), 
							json.getLong("Timestamp")) ){
						this.comsumer.tryCommit(json.getString("KafkaTopic"),json.getLong("KafkaOffset"));
					}
				});
			
			this.comsumer.run();
			LOG.info("task[{}] start consuming kafka topic '{}'",taskId,this.readerSliceConfig.getString(Job.KEY_TOPIC));
		}
		
		/*void addTaskIfAbsent(String database,long timestamp){
			if(!ExecutingTimestamps.containsKey(database) || 
					System.currentTimeMillis()-ExecutingTimestamps.get(database)
						>this.batch_interval ){
				startRead(database,timestamp);
			}
		}*/
		
		/**
		 * 仅当前startRead完成， 才会读取队列下一个execInfo
		 */
		public void startRead(String database,long timestamp){
			if(LOG.isDebugEnabled())
				LOG.debug("task[{}] begin process message {}@{} {}",taskId,database,timestamp,new Timestamp(timestamp).toString());
			if(Job.IgnoreTables.containsKey(database)){
				return;
			}
			String sql = this.getSql(database, timestamp);
			executeSql(sql,database,null,this.recordSender,super.getTaskPluginCollector(),this.fetchSize);
			
			if(LOG.isDebugEnabled())
				LOG.debug("task[{}] finish process message {}@{} {}",taskId,database,timestamp);
		}
		String getSql(String database,long timestamp){
			if(!ExecutingTimestamps.containsKey(database))//首次执行，开始时间为binlog时间戳
			{
				if(this.tsStart!=null){
					try {
						ExecutingTimestamps.put(database, tsFormat.parse(tsStart).getTime());
					} catch (ParseException e) {
						ExecutingTimestamps.put(database, timestamp-tsAdjust);//adjust 60s
					}
				}else
				    ExecutingTimestamps.put(database, timestamp-tsAdjust);//adjust 60s
			}
			
			String sql;
			long tsNow=System.currentTimeMillis();
			synchronized (ExecutingTimestamps){
				sql = this.querySql
					.replace("$database", database)
					.replace("$ts_start", new Timestamp(ExecutingTimestamps.get(database)).toString() )
					.replace("$ts_end", new Timestamp( tsNow).toString() );
				
				ExecutingTimestamps.put(database, tsNow);//下次执行时，开始时间从当前tsNow开始计算
			}
			return sql;
		}
		
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
