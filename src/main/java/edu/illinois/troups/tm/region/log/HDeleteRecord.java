package edu.illinois.troups.tm.region.log;

import static edu.illinois.troups.tm.log.TransactionLog.RECORD_TYPE_DELETE;
import edu.illinois.troups.tm.TID;
import edu.illinois.troups.tm.region.HKey;

public class HDeleteRecord extends HOperationRecord {

  public HDeleteRecord() {
    super(RECORD_TYPE_DELETE);
  }

  public HDeleteRecord(TID tid, HKey key) {
    super(RECORD_TYPE_DELETE, tid, key);
  }

}
