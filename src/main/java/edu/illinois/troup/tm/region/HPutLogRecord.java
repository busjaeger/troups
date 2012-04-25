package edu.illinois.troup.tm.region;

import edu.illinois.troup.tm.TID;
import edu.illinois.troup.tm.log.Log;

public class HPutLogRecord extends HOperationLogRecord {

  public HPutLogRecord() {
    super(Log.RECORD_TYPE_PUT);
  }

  public HPutLogRecord(long sid, TID tid, HKey groupKey, HKey key) {
    super(Log.RECORD_TYPE_PUT, sid, tid, groupKey, key);
  }

}
