package edu.illinois.htx.tm;

public interface KeyVersion<K extends Key> {

  K getKey();

  long getVersion();

}
