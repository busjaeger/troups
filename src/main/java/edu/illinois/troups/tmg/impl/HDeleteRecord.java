package edu.illinois.troups.tmg.impl;

import static edu.illinois.troups.tm.TransactionLog.RECORD_TYPE_DELETE;
import edu.illinois.troups.tm.TID;

public class HDeleteRecord extends HOperationRecord {

  public HDeleteRecord() {
    super(RECORD_TYPE_DELETE);
  }

  public HDeleteRecord(TID tid, HKey key) {
    super(RECORD_TYPE_DELETE, tid, key);
  }

}
