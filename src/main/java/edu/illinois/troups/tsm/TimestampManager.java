package edu.illinois.troups.tsm;

import java.io.IOException;
import java.util.Comparator;

public interface TimestampManager extends Comparator<Long> {

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

  // /**
  // * Can be used by clients to check whether they are the owner of the given
  // * timestamp.
  // *
  // * @param ts
  // * @return
  // */
  // boolean isHeldByCaller(long ts) throws NoSuchTimestampException,
  // IOException;

  boolean isReleased(long ts) throws IOException;

  // /**
  // *
  // * @param ts
  // * @param listener
  // * @return boolean if the timestamp existed and a listener was successfully
  // * set, false otherwise
  // * @throws IOException
  // */
  // boolean addTimestampListener(long ts, TimestampListener listener)
  // throws IOException;

  /**
   * Returns the last timestamp deleted
   * 
   * @return
   * @throws IOException
   */
  long getLastReclaimedTimestamp() throws IOException;

  void addTimestampReclamationListener(TimestampReclamationListener listener);

}