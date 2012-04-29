package edu.illinois.troups.tsm;

import java.io.IOException;
import java.util.List;


public interface ReclaimableTimestampManager extends TimestampManager {

  List<Long> getTimestamps() throws IOException;

  long getLastCreatedTimestamp() throws IOException;

  void setLastReclaimedTimestamp(long ts) throws IOException;

}