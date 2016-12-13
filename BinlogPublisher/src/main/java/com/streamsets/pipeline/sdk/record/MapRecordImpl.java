package com.streamsets.pipeline.sdk.record;

import com.google.common.base.Preconditions;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MapRecordImpl implements Record, Cloneable {
  private final HeaderImpl header;
  private final Map<String, Object> map;
  //Default true: so as to denote the record is just created
  //and initialized in a stage and did not pass through any other stage.
  private boolean isInitialRecord = true;

  // need default constructor for deserialization purposes (Kryo)
  private MapRecordImpl() {
    header = new HeaderImpl();
    map = new HashMap<>();
  }

  public MapRecordImpl(HeaderImpl header, Map<String, Object> value) {
    this.header = header;
    this.map = value;
  }

  public MapRecordImpl(String stageCreator, String recordSourceId, byte[] raw, String rawMime) {
    Preconditions.checkNotNull(stageCreator, "stage cannot be null");
    Preconditions.checkNotNull(recordSourceId, "source cannot be null");
    Preconditions.checkArgument((raw != null && rawMime != null) || (raw == null && rawMime == null),
                                "raw and rawMime have both to be null or not null");
    header = new HeaderImpl();
    if (raw != null) {
      header.setRaw(raw);
      header.setRawMimeType(rawMime);
    }
    header.setStageCreator(stageCreator);
    header.setSourceId(recordSourceId);
    map = new HashMap<>();
  }

  public MapRecordImpl(String stageCreator, Record originatorRecord, byte[] raw, String rawMime) {
    this(stageCreator, originatorRecord.getHeader().getSourceId(), raw, rawMime);
    String trackingId = originatorRecord.getHeader().getTrackingId();
    if (trackingId != null) {
      header.setTrackingId(trackingId);
    }
  }

  // for clone() purposes

  private MapRecordImpl(MapRecordImpl record) {
    Preconditions.checkNotNull(record, "record cannot be null");
    header = record.header.clone();
    map = (record.map != null) ? (Map<String, Object>)((HashMap<String, Object>)record.map).clone() : null;
    isInitialRecord = record.isInitialRecord();
  }

  public void addStageToStagePath(String stage) {
    Preconditions.checkNotNull(stage, "stage cannot be null");
    String currentPath = (header.getStagesPath() == null) ? "" : header.getStagesPath() + ":";
    header.setStagesPath(currentPath + stage);
  }

  public void createTrackingId() {
    String currentTrackingId = header.getTrackingId();
    String newTrackingId = getHeader().getSourceId() + "::" + getHeader().getStagesPath();
    if (currentTrackingId != null) {
      header.setPreviousTrackingId(currentTrackingId);
    }
    header.setTrackingId(newTrackingId);
  }

  public boolean isInitialRecord() {
    return isInitialRecord;
  }

  public void setInitialRecord(boolean isInitialRecord) {
    this.isInitialRecord = isInitialRecord;
  }

  @Override
  public HeaderImpl getHeader() {
    return header;
  }

  @Override
  public Field get() {
    return null;
  }

  @Override
  public Field set(Field field) {
    return field;
  }

  public Map<String, Object> getValue() {
    return this.map;
  }
  
  public Object getValue(String fieldPath) {
	  return this.map.get(fieldPath);
  }

  @Override
  public Field get(String fieldPath) {
	  return Field.create((String)this.map.get(fieldPath));
  }

	@Override
	public Field set(String fieldPath, Field newField) {
		this.map.put(fieldPath, newField.getValue());
		return newField;
	}
  @Override
  public Field delete(String fieldPath) {
	Object deleted  = this.map.get(fieldPath);
	this.map.remove(fieldPath);
    return Field.create((String)deleted);
  }

  @Override
  public boolean has(String fieldPath) {
    return this.map.containsKey(fieldPath);
  }

  @Override
  @Deprecated
  public Set<String> getFieldPaths() {
    return null;
  }

  @Override
  public Set<String> getEscapedFieldPaths() {
    return null;
  }

  @Override
  public String toString() {
    return Utils.format("Record[headers='{}' data='{}']", header, map);
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean equals(Object obj) {
    boolean eq = (this == obj);
    if (!eq && obj != null && obj instanceof MapRecordImpl) {
      MapRecordImpl other = (MapRecordImpl) obj;
      eq = header.equals(other.header);
      eq = eq && (this.map.equals(other.map));
    }
    return eq;
  }

  @Override
  public MapRecordImpl clone() {
    return new MapRecordImpl(this);
  }

  public void set(String fieldPath, Object newField) {
    this.map.put(fieldPath, newField);
  }

}