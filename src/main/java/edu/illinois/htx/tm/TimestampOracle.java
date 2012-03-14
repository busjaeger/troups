package edu.illinois.htx.tm;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

/**
 * TODO:
 * <ol>
 * <li>currently assumes failure free environment - need to support recovery</li>
 * <li>if we split transaction managers across servers, combine server ID with
 * logical time</li>
 * </ol>
 */
class TimestampOracle {

  private static final String path = "/tmp/current-timestamp";

  private long current;

  long next() {
    return current++;
  }

  void start() throws IOException {
    current = readCurrent();
  }

  void stop() throws IOException {
    writeCurrent(current);
  }

  private static long readCurrent() throws IOException {
    BufferedReader reader;
    try {
      reader = new BufferedReader(new FileReader(path));
    } catch (FileNotFoundException e) {
      return 0;
    }
    String line;
    try {
      line = reader.readLine();
    } finally {
      reader.close();
    }
    try {
      return Long.parseLong(line);
    } catch (NumberFormatException nfe) {
      throw new IOException("invalid timestamp file", nfe);
    }
  }

  private static void writeCurrent(long current) throws IOException {
    Writer writer = new FileWriter(path);
    try {
      writer.write(String.valueOf(current));
    } finally {
      writer.close();
    }
  }
}
