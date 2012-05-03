package edu.illinois.troups.util.perf;

import java.util.ArrayList;
import java.util.Collection;

public class Measurement {

  private String id;
  private long startTime;
  private long endTime;
  private Collection<Measurement> nestedMeasurements = new ArrayList<Measurement>();

  public Measurement() {
    this(null);
  }

  public Measurement(String id) {
    this.id = id;
    this.startTime = -1;
    this.endTime = -1;
    this.nestedMeasurements = new ArrayList<Measurement>();
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  public Collection<Measurement> getNestedMeasurements() {
    return nestedMeasurements;
  }

  public void setNestedMeasurements(Collection<Measurement> nestedMeasurements) {
    this.nestedMeasurements = nestedMeasurements;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append('[');
    builder.append("id=").append(id);
    builder.append(']');
    return builder.toString();
  }

}
