package edu.illinois.htx.tm;

import static edu.illinois.htx.tm.TransactionLogRecord.Type.ABORT;
import static edu.illinois.htx.tm.TransactionLogRecord.Type.BEGIN;
import static edu.illinois.htx.tm.TransactionLogRecord.Type.COMMIT;
import static edu.illinois.htx.tm.TransactionLogRecord.Type.DELETE;
import static edu.illinois.htx.tm.TransactionLogRecord.Type.READ;
import static edu.illinois.htx.tm.TransactionLogRecord.Type.WRITE;
import edu.illinois.htx.tm.TransactionLogRecord.Type;

/*
 * TODO: persist log and enable recovery
 */
public abstract class TransactionLog {

  void logBegin(long tts) {
    log(BEGIN, tts, null);
  }

  void logRead(long tts, Key key) {
    log(READ, tts, key);
  }

  void logWrite(long tts, Key key) {
    log(WRITE, tts, key);
  }

  void logDelete(long tts, Key key) {
    log(DELETE, tts, key);
  }

  void logAbort(long tts) {
    log(ABORT, tts, null);
  }

  void logCommit(long tts) {
    log(COMMIT, tts, null);
  }

  protected abstract void log(Type type, long tts, Key key);

}
