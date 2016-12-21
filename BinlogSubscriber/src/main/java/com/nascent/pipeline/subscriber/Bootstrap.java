package com.nascent.pipeline.subscriber;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Map;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordSender;
import com.nascent.pipeline.datax.mysql.KafkaBinlogBatchReader;
import com.nascent.pipeline.subscriber.xmltags.EventRoot;

public class Bootstrap {
	
	public static void main(String[] args)
	{
		/*
		KafkaBinlogBatchReader.Job job = new KafkaBinlogBatchReader.Job();
		job.init();
		
		KafkaBinlogBatchReader.Task task = new KafkaBinlogBatchReader.Task();
		task.prepare();
		task.startRead(new RecordSender(){

			@Override
			public Record createRecord() {
				return new Record(){

					@Override
					public void addColumn(Column arg0) {
						// TODO Auto-generated method stub
						
					}

					@Override
					public int getByteSize() {
						// TODO Auto-generated method stub
						return 0;
					}

					@Override
					public Column getColumn(int arg0) {
						// TODO Auto-generated method stub
						return null;
					}

					@Override
					public int getColumnNumber() {
						// TODO Auto-generated method stub
						return 0;
					}

					@Override
					public int getMemorySize() {
						// TODO Auto-generated method stub
						return 0;
					}

					@Override
					public void setColumn(int arg0, Column arg1) {
						// TODO Auto-generated method stub
						
					}
					
				};
			}

			@Override
			public void flush() {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void sendToWriter(Record arg0) {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void shutdown() {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void terminate() {
				// TODO Auto-generated method stub
				
			}
		});*/
		Bootstrap runner = new Bootstrap();
		runner.init();
	}
	
	void init(){
		
		KafkaBinlogConsumer.using("binlog2_slot".split(";"), json->{
			System.out.print(json.getString("KafkaTopic")+json.getLong("KafkaOffset"));
		}).run();
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
    
    public static <T> T unmarshal(InputStream xmlFile, Class<T> clazz) {
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
