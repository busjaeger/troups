package edu.illinois.troup.tsm.zk;

import java.io.IOException;
import java.util.List;

import edu.illinois.troup.tsm.TimestampManager;

public interface ReclaimableTimestampManager extends TimestampManager {

  List<Long> getTimestamps() throws IOException;

  long getLastCreatedTimestamp() throws IOException;

  void setLastReclaimedTimestamp(long ts) throws IOException;

}