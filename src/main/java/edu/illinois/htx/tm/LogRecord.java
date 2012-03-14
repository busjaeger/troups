package edu.illinois.htx.tm;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

class LogRecord implements Writable {

  static enum Type {
    BEGIN, READ, WRITE, DELETE, COMMIT, ABORT;
  }

  // Sequence ID
  final long sid;
  // Transaction ID
  final long tid;
  // Operation type
  final Type type;

  final Key key;

  LogRecord(long sid, long tid, Type type, Key key) {
    this.sid = sid;
    this.tid = tid;
    this.type = type;
    this.key = key;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    // TODO
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    // TODO
  }

}