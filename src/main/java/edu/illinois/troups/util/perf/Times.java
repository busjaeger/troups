package edu.illinois.troups.util.perf;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class Times {

  private final ConcurrentMap<String, Times> children = new ConcurrentHashMap<String, Times>();
  private final AtomicLong totalTime = new AtomicLong();
  private final AtomicLong hits = new AtomicLong();

  void update(Measurement measurement) {
    totalTime.addAndGet(measurement.getEndTime() - measurement.getStartTime());
    hits.incrementAndGet();
    for (Measurement nested : measurement.getNestedMeasurements()) {
      String id = nested.getId();
      Times child = children.get(id);
      if (child == null) {
        child = new Times();
        Times existing = children.putIfAbsent(id, child);
        if (existing != null)
          child = existing;
      }
      child.update(nested);
    }
  }

  public void write(File file) throws IOException {
    FileWriter writer = new FileWriter(file, true);
    try {
      write(writer);
    } finally {
      writer.close();
    }
  }

  public void write(Writer writer) throws IOException {
    write(0, writer, "root");
  }

  private void write(int tabs, Writer writer, String id) throws IOException {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < tabs; i++)
      builder.append('\t');
    builder.append(id);
    builder.append("[total: ").append(totalTime.get()).append("]");
    builder.append("[hits: ").append(hits.get()).append("]");
    builder.append("[average: ")
        .append(hits.get() == 0 ? 0 : totalTime.get() / hits.get()).append("]");
    builder.append('\n');
    writer.write(builder.toString());
    tabs++;
    for (Entry<String, Times> child : children.entrySet())
      child.getValue().write(tabs, writer, child.getKey());
  }

}
