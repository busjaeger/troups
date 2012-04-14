package edu.illinois.htx.tm;

public interface KeyVersions<K extends Key> {

  K getKey();

  Iterable<Long> getVersions();

}
