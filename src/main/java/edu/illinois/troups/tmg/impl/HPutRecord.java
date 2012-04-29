package edu.illinois.troups.tmg.impl;

import static edu.illinois.troups.tm.TransactionLog.RECORD_TYPE_PUT;
import edu.illinois.troups.tm.TID;

public class HPutRecord extends HOperationRecord {

  public HPutRecord() {
    super(RECORD_TYPE_PUT);
  }

  public HPutRecord(TID tid, HKey key) {
    super(RECORD_TYPE_PUT, tid, key);
  }

}
