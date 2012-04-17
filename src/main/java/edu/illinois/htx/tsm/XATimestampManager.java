package edu.illinois.htx.tsm;

import java.io.IOException;

/**
 * consider changing to more generic interface:
 * <p>
 * void setState(long ts, long pid, ParticipantState state) throws IOException;
 * <p>
 * ParticipantState getState(long ts, long pid) throws IOException;
 * <p>
 * 
 * 
 */
public interface XATimestampManager extends TimestampManager {

  public interface ConnectionListener {
    void disconnected(long ts);
  }

  public interface ParticipantListener {
    void disconnected(long pid);

    void prepared(long pid);

    void aborted(long pid);

    void committed(long pid);
  }

  /*
   * XA Manager also need test-and-set capabilities, since multiple participants
   * vote on the time-stamp state
   */
  void setState(long tid, TimestampState state, Version version)
      throws NoSuchTimestampException, VersionMismatchException, IOException;

  TimestampState getState(long tid, Version version)
      throws NoSuchTimestampException, IOException;

  /*
   * Participants
   */
  long join(long ts) throws IOException;

  void prepared(long ts, long pid) throws IOException;

  void committed(long ts, long pid) throws IOException;

  void aborted(long ts, long pid) throws IOException;

  void done(long ts, long pid) throws IOException;

  boolean addParticipantListener(long ts, long pid, ParticipantListener listener)
      throws IOException;

}