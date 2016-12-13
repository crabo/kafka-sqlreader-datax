package com.streamsets.pipeline.sdk;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Lists;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;

/**
 * direct output lane
 */
public class SingleLaneBatchMakerImpl implements BatchMaker {

  private final String outputLane;
  private final List<Record> laneToRecords;

  public SingleLaneBatchMakerImpl(String lane) {
    this.outputLane = lane==null?"DEFAULT_LANE":lane;
    laneToRecords = new ArrayList<>();
  }

  @Override
  public List<String> getLanes() {
    return Lists.newArrayList(outputLane);
  }

  @Override
  public void addRecord(Record record,String... lane) {
	  laneToRecords.add(record);
  }
  
  public void clear(){
	  laneToRecords.clear();
  }

  public List<Record> getOutput() {
    return laneToRecords;
  }
}