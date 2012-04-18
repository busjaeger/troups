package edu.illinois.htx.tsm.zk;

import java.io.IOException;

import edu.illinois.htx.tsm.NoSuchTimestampException;
import edu.illinois.htx.tsm.TimestampManager;

public interface ReclaimableTimestampManager extends TimestampManager {

  boolean hasReferences(long ts) throws NoSuchTimestampException, IOException;

  Iterable<Long> getTimestamps() throws IOException;

  long getLastCreatedTimestamp() throws IOException;

  void setLastReclaimedTimestamp(long ts) throws IOException;

}