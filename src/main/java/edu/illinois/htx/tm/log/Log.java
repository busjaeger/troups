package edu.illinois.htx.tm.log;

import java.io.IOException;
import java.util.Comparator;

import edu.illinois.htx.tm.TID;
import edu.illinois.htx.tm.Key;

public interface Log<K extends Key, R extends LogRecord>  extends Comparator<Long>{

  public static final int RECORD_TYPE_STATE_TRANSITION = 1;
  public static final int RECORD_TYPE_GET = 2;
  public static final int RECORD_TYPE_PUT = 3;
  public static final int RECORD_TYPE_DELETE = 4;

  public long appendStateTransition(TID tid, int state) throws IOException;

  public long appendGet(TID tid, K key, long version) throws IOException;

  public long appendPut(TID tid, K key) throws IOException;

  public long appendDelete(TID tid, K key) throws IOException;

  public abstract void truncate(long sid) throws IOException;

  public abstract Iterable<R> recover() throws IOException;

}