package com.nascent.pipeline.datax.mysql;

import java.util.concurrent.ArrayBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nascent.pipeline.datax.mysql.BatchExecutingInfo.ExecutingInfo;
import com.nascent.pipeline.subscriber.KafkaBinlogConsumer;

public class BatchExecutingInfo {
	public static class ExecutingInfo{
		public String database;
		//public String sqlKey;
		public long timestamp;
		
		public long addedTime;
		public ExecutingInfo(String database,long timestamp){
			this.database=database;
			//this.sqlKey=sqlKey;
			this.timestamp=timestamp;
			this.addedTime=System.currentTimeMillis();
		}
		
		@Override
	    public int hashCode() {
	        return database.hashCode();
	    }
	    @Override
	    public boolean equals(Object obj) {
	        return obj.hashCode()==this.hashCode();
	    }
	}
	
	
	private static final Logger LOGGER = LoggerFactory.getLogger(BatchExecutingInfo.class);
	final ArrayBlockingQueue<ExecutingInfo> queue;
	final KafkaBinlogBatchReader.Task task;
	final int batch_interval_ms;
	public BatchExecutingInfo(KafkaBinlogBatchReader.Task task,int batchMs,ArrayBlockingQueue<ExecutingInfo> q){
		this.queue=q;//足够多的待处理数据，防止<10s 的同一msg先后出入队列；
		this.task = task;
		this.batch_interval_ms = batchMs;//合并10s内的多个请求
		
		startDaemon();
	}
	
	int i=0;
	public boolean addTaskIfAbsent(String database,long timestamp){
		ExecutingInfo info = new ExecutingInfo(database,timestamp);
		if(!queue.contains(info))
		{
			try {
				queue.put(info);//持续等待，直至队列数<100
				i++;
				
				if(i>100){
					i=0;
					
					LOGGER.info("added kafka message {}@{}",timestamp,database);
				}else if(LOGGER.isDebugEnabled())
					LOGGER.debug("added kafka message {}@{}",timestamp,database);
				
				return true;
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		return false;
	}
	
	/**
	 * 未达到10s等待时间的数据不允许出队列
	 */
	private void daemonCheck() throws InterruptedException{
		if(queue.isEmpty()){
			Thread.sleep(2000);
		}
		//统一个select 至少间隔时间，如10s
		else if(System.currentTimeMillis() - queue.peek().addedTime>this.batch_interval_ms){
			//仅当前startRead完成， 才会读取队列下一个execInfo
			ExecutingInfo e = queue.poll();
			this.task.startRead(e.database,e.timestamp);
		}
		else{
			Thread.sleep(500);
			//if(LOGGER.isTraceEnabled())
			//	LOGGER.trace("queued {} tasks are not pending for {}s",queue.size(),batch_interval_sec);
		}
	}
	
	Thread daemonThread;
	private void startDaemon(){
		Runnable daemonTask = new Runnable() {
            public void run() {
            	while(true){
            		try{
            			daemonCheck();
	            	} catch (InterruptedException e) {
	    				e.printStackTrace();
	    			}
            	}
            }
        };

        daemonThread = new Thread(daemonTask);
        daemonThread.setDaemon(true);
        daemonThread.setName("msg-queue");
        daemonThread.start();
	}
}
