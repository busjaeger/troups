package edu.illinois.htx.tm;

public interface LifecycleListener {

  void starting();

  void started();

  void stopping();

  void aborting();

  void stopped();
}
