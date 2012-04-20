package edu.illinois.htx.tm.region;

import edu.illinois.htx.tm.TID;
import edu.illinois.htx.tm.log.Log;

public class HPutLogRecord extends HOperationLogRecord {

  public HPutLogRecord() {
    super(Log.RECORD_TYPE_PUT);
  }

  public HPutLogRecord(long sid, TID tid, HKey key) {
    super(Log.RECORD_TYPE_PUT, sid, tid, key);
  }

}
