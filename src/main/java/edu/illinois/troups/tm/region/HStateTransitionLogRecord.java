package edu.illinois.troups.tm.region;

import static edu.illinois.troups.tm.log.Log.RECORD_TYPE_STATE_TRANSITION;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.illinois.troups.tm.TID;
import edu.illinois.troups.tm.log.StateTransitionLogRecord;

public class HStateTransitionLogRecord extends HLogRecord implements
    StateTransitionLogRecord<HKey> {

  private int state;

  HStateTransitionLogRecord() {
    super(RECORD_TYPE_STATE_TRANSITION);
  }

  HStateTransitionLogRecord(long sid, TID tid, HKey groupKey, int state) {
    super(RECORD_TYPE_STATE_TRANSITION, sid, tid, groupKey);
    this.state = state;
  }

  HStateTransitionLogRecord(int type) {
    super(type);
  }

  HStateTransitionLogRecord(int type, long sid, TID tid, HKey groupKey,
      int state) {
    super(type, sid, tid, groupKey);
    this.state = state;
  }

  @Override
  public int getTransactionState() {
    return state;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    state = in.readInt();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeInt(state);
  }

}
