package edu.illinois.troups.tsm.zk;

import java.io.IOException;
import java.util.List;

import edu.illinois.troups.tsm.TimestampManager;

public interface ReclaimableTimestampManager extends TimestampManager {

  List<Long> getTimestamps() throws IOException;

  long getLastCreatedTimestamp() throws IOException;

  void setLastReclaimedTimestamp(long ts) throws IOException;

}