package edu.illinois.htx.tm.region;

import edu.illinois.htx.tm.log.Log;

public class HDeleteLogRecord extends HOperationLogRecord {

  public HDeleteLogRecord() {
    super(Log.RECORD_TYPE_DELETE);
  }

  public HDeleteLogRecord(long sid, long tid, HKey key) {
    super(Log.RECORD_TYPE_DELETE, sid, tid, key);
  }

}
