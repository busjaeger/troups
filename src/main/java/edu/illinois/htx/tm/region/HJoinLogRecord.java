package edu.illinois.htx.tm.region;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.illinois.htx.tm.XATransactionState;
import edu.illinois.htx.tm.log.JoinLogRecord;
import edu.illinois.htx.tm.log.XALog;

public class HJoinLogRecord extends HStateTransitionLogRecord implements
    JoinLogRecord {

  private long pid;

  HJoinLogRecord() {
    super(XALog.RECORD_TYPE_JOIN);
  }

  HJoinLogRecord(long sid, long tid, long pid) {
    super(XALog.RECORD_TYPE_JOIN, sid, tid, XATransactionState.JOINED);
  }

  @Override
  public long getPID() {
    return pid;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    pid = in.readLong();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeLong(pid);
  }
}
