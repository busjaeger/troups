package edu.illinois.htx.tm;

import static edu.illinois.htx.tm.LogRecord.Type.ABORT;
import static edu.illinois.htx.tm.LogRecord.Type.BEGIN;
import static edu.illinois.htx.tm.LogRecord.Type.COMMIT;
import static edu.illinois.htx.tm.LogRecord.Type.DELETE;
import static edu.illinois.htx.tm.LogRecord.Type.READ;
import static edu.illinois.htx.tm.LogRecord.Type.WRITE;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import edu.illinois.htx.tm.LogRecord.Type;

/*
 * TODO: persist log and enable recovery
 */
class TransactionLog {

  private final List<LogRecord> log;
  private final AtomicLong sid;

  TransactionLog() {
    this.log = new ArrayList<LogRecord>();
    this.sid = new AtomicLong(0L);
  }

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

  private void log(Type type, long tts, Key key) {
    log.add(new LogRecord(sid.incrementAndGet(), tts, type, key));
  }

}
