package com.nascent.pipeline.datax.mysql;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

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
import com.nascent.pipeline.subscriber.KafkaBinlogConsumer;
import com.nascent.pipeline.subscriber.xmltags.EventRoot;

public class KafkaBinlogReader extends Reader {
	public static class Job extends Reader.Job {
		public static final String KEY_TOPIC="topics";
		
		public static EventRoot EventPolicy;
        private static final Logger LOG = LoggerFactory
                .getLogger(Job.class);
        private Configuration originConfig = null;
		
        @Override
		public void init() {
			this.originConfig = this.getPluginJobConf();
			EventPolicy=loadPolicy();
			EventPolicy.init();
		}

        
        @Override
		public void prepare() {
        	
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
			if(patitions.length>1){
				for (int i=0;i<patitions.length;i++) {//按照channel建立足够多的kafka连接
					Configuration splitedConfig = this.originConfig.clone();
					splitedConfig.set(KEY_TOPIC, patitions[i]);
					
					readerSplitConfigs.add(splitedConfig);
				}
			}else
			{
				for (int i=0;i<adviceNumber;i++) {//按照channel建立足够多的kafka连接
					Configuration splitedConfig = this.originConfig.clone();
					readerSplitConfigs.add(splitedConfig);
				}
			}
			return readerSplitConfigs;
		}
		
		private EventRoot loadPolicy(){
			try {
				InputStream xml = new FileInputStream(System.getProperty("user.dir")+"/conf/pipeline.xml");
				return unmarshal(xml,EventRoot.class);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
			return null;
	    }
	    
	    private static <T> T unmarshal(InputStream xmlFile, Class<T> clazz) {
	        T result = null;
	        try {
	          Unmarshaller avm = JAXBContext.newInstance(clazz).createUnmarshaller();
	          result = (T) avm.unmarshal(xmlFile);
	        } catch (JAXBException e) {
	          e.printStackTrace();
	        }
	        return result;
	      }
	}
	
	public static class Task extends Reader.Task {
		private static Logger LOG = LoggerFactory.getLogger(Task.class);

		private Configuration readerSliceConfig;

		private String username;
        private String password;
        private String jdbcUrl;
        
        private DataBaseType dataBaseType;
        private int taskGroupId = -1;
        private int taskId=-1;
        KafkaBinlogConsumer comsumer;
        
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
		}

		@Override
		public void prepare() {
			
		}

		@Override
		public void post() {

		}

		@Override
		public void destroy() {
			if(comsumer!=null)
				comsumer.shutdown();
		}

		@Override
		public void startRead(RecordSender recordSender) {
			int fetchSize = this.readerSliceConfig.getInt("fetch_size",1000);
			this.comsumer = KafkaBinlogConsumer.using(
					this.readerSliceConfig.getString(Job.KEY_TOPIC,"")
					.split(","), json->{
				Job.EventPolicy.route(
					json.getString("Database"), 
					json.getString("Table"),
					json.getString("Type"),
					json);
				
				//Mapping result
				if(json.containsKey("Transformer")){
					String basicMsg=json.getString("Database")+"."+json.getString("Table")
						+"@"+json.getString("Timestamp");
					
					startRead(
							json.getString("Transformer"),
							basicMsg,
							readerSliceConfig,recordSender,
							super.getTaskPluginCollector(),fetchSize);
				}else{
					//TODO: parse Map<> to Record
				}
			});
		}
		
		public void startRead(String querySql,String basicMsg,Configuration readerSliceConfig,
                RecordSender recordSender,
                TaskPluginCollector taskPluginCollector, int fetchSize) {
		
			PerfTrace.getInstance().addTaskDetails(taskId, basicMsg);
			LOG.debug("Begin to read record by Sql: [{}\n] {}.",querySql, basicMsg);
			PerfRecord queryPerfRecord = new PerfRecord(taskGroupId,taskId, PerfRecord.PHASE.SQL_QUERY);
			queryPerfRecord.start();
			
			Connection conn = DBUtil.getConnection(this.dataBaseType, jdbcUrl,
			      username, password);
			
			// session config .etc related
			DBUtil.dealWithSessionConfig(conn, readerSliceConfig,
			      this.dataBaseType, basicMsg);
			
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
			              metaData, columnNumber, null, taskPluginCollector);
			      lastTime = System.nanoTime();
			  }
			
			  allResultPerfRecord.end(rsNextUsedTime);
			  //目前大盘是依赖这个打印，而之前这个Finish read record是包含了sql查询和result next的全部时间
			  LOG.debug("Finished read record by Sql: [{}\n] {}.",
			          querySql, basicMsg);
			
			}catch (Exception e) {
			  throw DataXException
              	.asDataXException(GenericErrorCode.ERROR,
              			"readRecord error: "+basicMsg,e);
			} finally {
			  DBUtil.closeDBResources(null, conn);
			}
		}
		
		protected Record transportOneRecord(RecordSender recordSender, ResultSet rs, 
                ResultSetMetaData metaData, int columnNumber, String mandatoryEncoding, 
                TaskPluginCollector taskPluginCollector) {
            Record record = buildRecord(recordSender,rs,metaData,columnNumber,mandatoryEncoding,taskPluginCollector); 
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
