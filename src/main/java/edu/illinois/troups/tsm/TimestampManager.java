package edu.illinois.troups.tsm;

import java.io.IOException;
import java.util.Comparator;

public interface TimestampManager extends Comparator<Long> {

  public static final long VERSION = 1L;

  public interface TimestampListener {
    void released(long ts);
  }

  public interface TimestampReclamationListener {
    /**
     * Invoked with the latest timestamp reclaimed
     * 
     * @param ts
     */
    void reclaimed(long ts);
  }

  /**
   * Creates a new timestamp. The timestamp exists until it is deleted
   * explicitly via {@link TimestampManager#release(long)} or until a certain
   * timeout period after the creating process crashed.
   * 
   * @return
   * @throws IOException
   */
  long acquire() throws IOException;

  /**
   * Deletes the given timestamp.
   * 
   * @param ts
   * @return true if timestamp existed and was successfully removed, false
   *         otherwise
   * @throws IOException
   */
  boolean release(long ts) throws IOException;

  /**
   * Returns the last timestamp deleted
   * 
   * @return
   * @throws IOException
   */
  long getLastReclaimedTimestamp() throws IOException;

  void addTimestampReclamationListener(TimestampReclamationListener listener);

}