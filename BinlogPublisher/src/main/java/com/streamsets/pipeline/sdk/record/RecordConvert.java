package com.streamsets.pipeline.sdk.record;

import java.util.List;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.stage.origin.mysql.EnrichedEvent;

public interface RecordConvert {
	List<Record> toRecords(EnrichedEvent event);
}
