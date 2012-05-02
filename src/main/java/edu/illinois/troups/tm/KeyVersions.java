package edu.illinois.troups.tm;


public interface KeyVersions<K extends Key> {

  K getKey();

  Iterable<Long> getVersions();

}
