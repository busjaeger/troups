package edu.illinois.htx.tsm;

import java.io.IOException;

public interface XATimestampManager extends TimestampManager {

  public interface CoordinatorListener {
    void prepare();

    void abort();

    void commit();
  }

  public interface ParticipantListener {
    void prepared();

    void aborted();

    void committed();
  }

  long join(long ts);

  void prepared(long ts, long pid) throws IOException;

  void done(long ts, long pid) throws IOException;

  boolean addListener(long ts, long pid, ParticipantListener listener)
      throws IOException;

  boolean addListener(long ts, CoordinatorListener listener) throws IOException;

}