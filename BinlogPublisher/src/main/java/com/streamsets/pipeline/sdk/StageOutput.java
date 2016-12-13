package com.streamsets.pipeline.sdk;

import java.util.List;
import java.util.Map;
import java.util.Collections;

import com.streamsets.pipeline.api.Record;

public class StageOutput {

	private final String newOffset;
    private final Map<String, List<Record>> records;

    public StageOutput(String newOffset, Map<String, List<Record>> records) {
      this.newOffset = newOffset;
      for (Map.Entry<String, List<Record>> entry : records.entrySet()) {
        entry.setValue(Collections.unmodifiableList(entry.getValue()));
      }
      this.records = Collections.unmodifiableMap(records);
    }

    public String getNewOffset() {
      return newOffset;
    }

    public Map<String, List<Record>> getRecords() {
      return records;
    }
}
