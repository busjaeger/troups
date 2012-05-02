package edu.illinois.troups.tm.region.log;

import static edu.illinois.troups.tm.log.TransactionLog.RECORD_TYPE_PUT;

import java.util.List;

import edu.illinois.troups.tm.TID;
import edu.illinois.troups.tm.region.HKey;

public class HPutRecord extends HOperationRecord {

  public HPutRecord() {
    super(RECORD_TYPE_PUT);
  }

  public HPutRecord(TID tid, List<HKey> keys) {
    super(RECORD_TYPE_PUT, tid, keys);
  }

}
