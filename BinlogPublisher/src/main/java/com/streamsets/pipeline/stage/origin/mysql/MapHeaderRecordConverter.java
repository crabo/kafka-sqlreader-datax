/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.origin.mysql;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.EventHeader;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.sdk.record.MapRecordImpl;
import com.streamsets.pipeline.sdk.record.RecordConvert;
import com.streamsets.pipeline.stage.origin.mysql.schema.Column;
import com.streamsets.pipeline.stage.origin.mysql.schema.ColumnValue;
import com.streamsets.pipeline.stage.origin.mysql.schema.Table;

public class MapHeaderRecordConverter  implements RecordConvert {
	private static final Logger LOG = LoggerFactory.getLogger(MapHeaderRecordConverter.class);
	
  static final String TYPE_FIELD = "Type";
  static final String DATABASE_FIELD = "Database";
  static final String TABLE_FIELD = "Table";
  static final String BIN_LOG_FILENAME_FIELD = "BinLogFilename";
  static final String BIN_LOG_POSITION_FIELD = "BinLogPosition";
  static final String GTID_FIELD = "GTID";
  static final String EVENT_SEQ_NO_FIELD = "SeqNo";
  static final String OFFSET_FIELD = "Offset";
  static final String SERVER_ID_FIELD = "ServerId";
  static final String TIMESTAMP_ID_FIELD = "Timestamp";
  static final String DATA_FIELD = "Data";
  static final String OLD_DATA_FIELD = "OldData";
  final int mergeEventMs;

  
  static Record createRecord(Map<String, Object> value){
	  return new MapRecordImpl(null,value);
  }

  public MapHeaderRecordConverter(int intervalEventMs) {
	  if(intervalEventMs<100) intervalEventMs=100;
	  this.mergeEventMs = intervalEventMs;
  }
  
  public static class ExecutingInfo{
		public String database;
		public EnrichedEvent event;
		
		public long addedTime;
		public ExecutingInfo(String database,EnrichedEvent event){
			this.database=database;
			this.event=event;
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

  ArrayDeque<ExecutingInfo> SendingRecords=new ArrayDeque<>();
  /** by crabo !!!
   * 同一table的事件，3秒内重复的都合并为一个事件
   * @param event
   * @return
   */
  public List<Record> toRecords(EnrichedEvent event){
	  Table tbl = event.getTable();
	  String key=tbl.getDatabase()+tbl.getName();
	  
	  ExecutingInfo exec = new ExecutingInfo(key,event);
	  if(!SendingRecords.contains(exec))
		  SendingRecords.add(exec);
	  
	  List<Record> li =null;
	  exec = SendingRecords.peek();
	  while(exec!=null &&
			  System.currentTimeMillis()-exec.addedTime>mergeEventMs){
		  if(li==null)
			  li = doToRecords(SendingRecords.pop().event);
		  else{
			  li.addAll(doToRecords(SendingRecords.pop().event));
		  }
		  
		  exec = SendingRecords.peek();
	  }
	  return li;
  }
  
  private List<Record> doToRecords(EnrichedEvent event) {
	
    EventType eventType = event.getEvent().getHeader().getEventType();
    if(LOG.isDebugEnabled()){
    	LOG.debug("binlog event '{}' at {}.{}@{}",eventType,
    			event.getTable().getDatabase(),event.getTable().getName(),
    			event.getEvent().getHeader().getTimestamp());
    }
    switch (eventType) {
      case PRE_GA_WRITE_ROWS:
      case WRITE_ROWS:
      case EXT_WRITE_ROWS:
        return toRecords(
            event.getTable(),
            event.getEvent().getHeader(),
            event.getEvent().<WriteRowsEventData>getData(),
            event.getOffset()
        );
      case PRE_GA_UPDATE_ROWS:
      case UPDATE_ROWS:
      case EXT_UPDATE_ROWS:
        return toRecords(
            event.getTable(),
            event.getEvent().getHeader(),
            event.getEvent().<UpdateRowsEventData>getData(),
            event.getOffset()
        );
      case PRE_GA_DELETE_ROWS:
      case DELETE_ROWS:
      case EXT_DELETE_ROWS:
        return toRecords(
            event.getTable(),
            event.getEvent().getHeader(),
            event.getEvent().<DeleteRowsEventData>getData(),
            event.getOffset()
        );
      default:
    	  /*by crabo*/
    	  QueryEventData evn=event.getEvent().<QueryEventData>getData();
    	  if(evn!=null){
	          Map<String, Object> fields = createHeader(event.getTable(), event.getEvent().getHeader(), event.getOffset());
	          fields.put(TYPE_FIELD,"QUERY");
	          
	         /* Map<String, Object> data = new HashMap<>(1);
	          data.put("Sql", evn.getSql());
	          
	          fields.put(DATA_FIELD, data);*/
	          
	          List<Record> res = new ArrayList<>();
	          res.add(createRecord(fields));
	          return res;
    	  }
    	  
    	  throw new IllegalArgumentException(String.format("EventType '%s' not supported", eventType));
    }
  }


  private List<Record> toRecords(Table table,
                                 EventHeader eventHeader,
                                 UpdateRowsEventData eventData,
                                 SourceOffset offset) {
    List<Record> res = new ArrayList<>(eventData.getRows().size());
    for (Map.Entry<Serializable[], Serializable[]> row : eventData.getRows()) {

      Map<String, Object> fields = createHeader(table, eventHeader, offset);
      fields.put(TYPE_FIELD, "UPDATE");

      /*Map<String,Object> columnValuesOld = zipColumnsValues(
          eventData.getIncludedColumnsBeforeUpdate(),
          table,
          row.getKey()
      );
      fields.put(OLD_DATA_FIELD, columnValuesOld);

      Map<String,Object> columnValues = zipColumnsValues(
          eventData.getIncludedColumns(),
          table,
          row.getValue()
      );
      fields.put(DATA_FIELD, columnValues);*/

      res.add(createRecord(fields));
    }
    return res;
  }

  private List<Record> toRecords(Table table,
                                 EventHeader eventHeader,
                                 WriteRowsEventData eventData,
                                 SourceOffset offset) {
    List<Record> res = new ArrayList<>(eventData.getRows().size());
    for (Serializable[] row : eventData.getRows()) {
      Map<String, Object> fields = createHeader(table, eventHeader, offset);
      fields.put(TYPE_FIELD, "INSERT");

      /*Map<String,Object> columnValues = zipColumnsValues(
          eventData.getIncludedColumns(),
          table,
          row
      );
      fields.put(DATA_FIELD, columnValues);*/

      res.add(createRecord(fields));
    }
    return res;
  }

  private List<Record> toRecords(Table table,
                                 EventHeader eventHeader,
                                 DeleteRowsEventData eventData,
                                 SourceOffset offset) {
    List<Record> res = new ArrayList<>(eventData.getRows().size());
    for (Serializable[] row : eventData.getRows()) {
      Map<String, Object> fields = createHeader(table, eventHeader, offset);
      fields.put(TYPE_FIELD, "DELETE");

      /*Map<String, Object> columnValues = zipColumnsValues(eventData.getIncludedColumns(), table, row);
      fields.put(OLD_DATA_FIELD, columnValues);*/

      res.add(createRecord(fields));
    }
    return res;
  }


  private Map<String, Object> createHeader(Table table, EventHeader header, SourceOffset offset) {
	Map<String, Object> map = new HashMap<>(8);//======  长度7+1 ====！！！！！
    map.put(DATABASE_FIELD, table.getDatabase());
    map.put(TABLE_FIELD, table.getName());
    map.put(OFFSET_FIELD, offset.format());
    map.put(SERVER_ID_FIELD, header.getServerId());
    map.put(TIMESTAMP_ID_FIELD, header.getTimestamp());

    if (offset instanceof BinLogPositionSourceOffset) {
      BinLogPositionSourceOffset bo = (BinLogPositionSourceOffset) offset;
      map.put(BIN_LOG_FILENAME_FIELD, bo.getFilename());
      map.put(BIN_LOG_POSITION_FIELD, bo.getPosition());
    } else if (offset instanceof GtidSourceOffset) {
      GtidSourceOffset go = (GtidSourceOffset) offset;
      map.put(GTID_FIELD, go.getGtid());
      map.put(EVENT_SEQ_NO_FIELD, go.getSeqNo());
    }

    return map;
  }

  private Map<String, Field> toMap(List<ColumnValue> columnValues) {
    Map<String, Field> data = new HashMap<>(columnValues.size());
    for (ColumnValue cv : columnValues) {
      String name = cv.getHeader().getName();
      Field value = cv.getHeader().getType().toField(cv.getValue());
      data.put(name, value);
    }
    return data;
  }

  private Map<String,Object> zipColumnsValues(BitSet columns, Table table, Serializable[] values) {
	Map<String,Object> res = new HashMap<>(columns.size());
    int n = 0;
    for (int i = 0; i < columns.size(); i++) {
      if (columns.get(i)) {
        Column col = table.getColumn(i);
        res.put(col.getName(), values[n]);
        n++;
      }
    }
    return res;
  }
}
