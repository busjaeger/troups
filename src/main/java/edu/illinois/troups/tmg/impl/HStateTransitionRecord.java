package edu.illinois.troups.tmg.impl;

import static edu.illinois.troups.tm.TransactionLog.RECORD_TYPE_STATE_TRANSITION;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.illinois.troups.tm.TID;
import edu.illinois.troups.tm.TransactionLog.StateTransitionRecord;

public class HStateTransitionRecord extends HRecord implements
    StateTransitionRecord<HKey> {

  private int state;

  HStateTransitionRecord() {
    super(RECORD_TYPE_STATE_TRANSITION);
  }

  HStateTransitionRecord(TID tid, int state) {
    super(RECORD_TYPE_STATE_TRANSITION, tid);
    this.state = state;
  }

  HStateTransitionRecord(int type) {
    super(type);
  }

  HStateTransitionRecord(int type, TID tid, int state) {
    super(type, tid);
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
