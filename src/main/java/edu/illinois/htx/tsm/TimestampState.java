package edu.illinois.htx.tsm;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Writable;

public class TimestampState implements Writable {

  private boolean done = false;
  private Map<Long, Boolean> votes;

  public TimestampState() {
  }

  public TimestampState(byte[] bytes) {
    DataInputBuffer in = new DataInputBuffer();
    in.reset(bytes, bytes.length);
    try {
      readFields(in);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public TimestampState(boolean done) {
    this(done, null);
  }

  public TimestampState(boolean done, Map<Long, Boolean> votes) {
    this.done = done;
    this.votes = votes;
  }

  public void setDone(boolean done) {
    this.done = done;
  }

  public boolean isDone() {
    return done;
  }

  public boolean hasActiveParticipants() {
    if (votes == null)
      return false;
    for (Boolean vote: votes.values())
      if (!vote)
        return true;
    return false;
  }

  public void setVotes(Map<Long, Boolean> votes) {
    this.votes = votes;
  }

  public Map<Long, Boolean> getVotes() {
    return votes;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeBoolean(done);
    out.writeInt(votes == null ? -1 : votes.size());
    if (votes != null) {
      for (Entry<Long, Boolean> entry : votes.entrySet()) {
        out.writeLong(entry.getKey());
        out.writeBoolean(entry.getValue());
      }
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    done = in.readBoolean();
    int size = in.readInt();
    if (size > -1) {
      votes = new TreeMap<Long, Boolean>();
      for (int i = 0; i < size; i++) {
        long part = in.readLong();
        boolean partDone = in.readBoolean();
        votes.put(part, partDone);
      }
    }
  }

}
