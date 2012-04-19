package edu.illinois.htx.tm.region;

import static edu.illinois.htx.tm.log.Log.RECORD_TYPE_STATE_TRANSITION;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.illinois.htx.tm.log.StateTransitionLogRecord;

public class HStateTransitionLogRecord extends HLogRecord implements
    StateTransitionLogRecord {

  private int state;

  HStateTransitionLogRecord() {
    super(RECORD_TYPE_STATE_TRANSITION);
  }

  HStateTransitionLogRecord(long sid, long tid, int state) {
    super(RECORD_TYPE_STATE_TRANSITION, sid, tid);
    this.state = state;
  }

  HStateTransitionLogRecord(int type) {
    super(type);
  }

  HStateTransitionLogRecord(int type, long sid, long tid, int state) {
    super(type, sid, tid);
    this.state = state;
  }

  @Override
  public int getTransactionState() {
    return state;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    state = in.readInt();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(state);
  }

}
