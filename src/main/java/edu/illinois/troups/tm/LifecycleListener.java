package edu.illinois.troups.tm;

public interface LifecycleListener {

  void starting();

  void started();

  void stopping();

  void aborting();

  void stopped();
}
