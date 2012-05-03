package edu.illinois.troups.util.perf;

public class ThreadLocalStopWatch {

  private static final ThreadLocal<StopWatch> threadStopWatch = new ThreadLocal<StopWatch>();

  public static void set(StopWatch stopWatch) {
    threadStopWatch.set(stopWatch);
  }

  public static void remove() {
    threadStopWatch.remove();
  }

  public static void start(Times times) {
    if (times != null) {
      StopWatch stopWatch = new StopWatch(null, "root");
      ThreadLocalStopWatch.set(stopWatch);
    }
  }

  public static void stop(Times times) {
    if (times != null) {
      StopWatch stopWatch = threadStopWatch.get();
      ThreadLocalStopWatch.remove();
      stopWatch.stop();
      times.update(stopWatch.getMeasurement());
    }
  }

  public static void start(String id) {
    StopWatch stopWatch = threadStopWatch.get();
    if (stopWatch != null)
      threadStopWatch.set(stopWatch.start(id));
  }

  public static void stop() {
    StopWatch stopWatch = threadStopWatch.get();
    if (stopWatch != null)
      threadStopWatch.set(stopWatch.stop());
  }

}
