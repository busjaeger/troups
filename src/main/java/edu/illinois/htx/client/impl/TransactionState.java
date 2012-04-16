package edu.illinois.htx.client.impl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Writable;

public class TransactionState implements Writable {

  private boolean done = false;
  private Map<Integer, Boolean> votes;

  public TransactionState() {
  }

  public TransactionState(boolean done) {
    this(done, null);
  }

  public TransactionState(boolean done, Map<Integer, Boolean> votes) {
    this.done = done;
    this.votes = votes;
  }

  public void setDone(boolean done) {
    this.done = done;
  }

  public boolean isDone() {
    return done;
  }

  public void setVotes(Map<Integer, Boolean> votes) {
    this.votes = votes;
  }

  public Map<Integer, Boolean> getVotes() {
    return votes;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeBoolean(done);
    out.writeInt(votes == null ? -1 : votes.size());
    if (votes != null) {
      for (Entry<Integer, Boolean> entry : votes.entrySet()) {
        out.writeInt(entry.getKey());
        out.writeBoolean(entry.getValue());
      }
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    done = in.readBoolean();
    int size = in.readInt();
    if (size > -1) {
      votes = new TreeMap<Integer, Boolean>();
      for (int i = 0; i < size; i++) {
        int part = in.readInt();
        boolean partDone = in.readBoolean();
        votes.put(part, partDone);
      }
    }
  }

  public void readFields(byte[] bytes) {
    DataInputBuffer in = new DataInputBuffer();
    in.reset(bytes, bytes.length);
    try {
      readFields(in);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
