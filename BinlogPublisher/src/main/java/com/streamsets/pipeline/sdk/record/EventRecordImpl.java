package com.streamsets.pipeline.sdk.record;

import com.streamsets.pipeline.api.EventRecord;

import java.util.Date;

public class EventRecordImpl extends RecordImpl implements EventRecord {

  public EventRecordImpl(String type, int version, String stageCreator, String recordSourceId, byte[] raw, String rawMime) {
    super(stageCreator, recordSourceId, raw, rawMime);
    setEventAtributes(type, version);
  }

  private void setEventAtributes(String type, int version) {
    getHeader().setAttribute(EventRecord.TYPE, type);
    getHeader().setAttribute(EventRecord.VERSION, String.valueOf(version));
    getHeader().setAttribute(EventRecord.CREATION_TIMESTAMP, String.valueOf(System.currentTimeMillis()));
  }

}
