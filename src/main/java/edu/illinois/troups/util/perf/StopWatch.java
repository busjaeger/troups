package edu.illinois.troups.util.perf;

import edu.illinois.troups.util.perf.Measurement;
import edu.illinois.troups.util.perf.StopWatch;

public class StopWatch  {

  private final StopWatch parent;
  private final Measurement measurement;

  public StopWatch(StopWatch parent, String id) {
    this.parent = parent;
    this.measurement = new Measurement(id);
    this.measurement.setStartTime(System.currentTimeMillis());
  }

  public StopWatch start(String id) {
    StopWatch stopWatch = new StopWatch(this, id);
    measurement.getNestedMeasurements().add(stopWatch.measurement);
    return stopWatch;
  }

  public StopWatch stop() {
    measurement.setEndTime(System.currentTimeMillis());
    return parent;
  }

  public Measurement getMeasurement() {
    return measurement;
  }

}
