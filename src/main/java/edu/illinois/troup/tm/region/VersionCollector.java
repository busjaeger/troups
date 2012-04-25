package edu.illinois.troup.tm.region;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.InternalScanner;

// TODO delete unused versions during compact
public class VersionCollector implements InternalScanner {

  private final InternalScanner internalScanner;
  private final long lrt;

  public VersionCollector(InternalScanner internalScanner, long ldt) {
    this.internalScanner = internalScanner;
    this.lrt = ldt;
  }

  @Override
  public boolean next(List<KeyValue> results) throws IOException {
    return next(results, -1);
  }

  @Override
  public boolean next(List<KeyValue> result, int limit) throws IOException {
    return internalScanner.next(result, limit);
  }

  @Override
  public void close() throws IOException {
    internalScanner.close();
  }

}
