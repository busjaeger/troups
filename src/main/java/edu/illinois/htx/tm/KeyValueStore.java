package edu.illinois.htx.tm;

import java.io.IOException;

public interface KeyValueStore<T extends Key> {

  /**
   * Deletes a specific version from this KeyValueStore. Operation is expected
   * to be idempotent.
   * 
   * @param key
   * @param version
   */
  void deleteVersion(T key, long version) throws IOException;

  /**
   * Deletes all versions older than the given version (inclusive). If operation
   * is not atomic, it must start with the oldest. Operation is expected to be
   * idempotent.
   * 
   * @param key
   * @param version
   */
  void deleteVersions(T key, long version) throws IOException;

}