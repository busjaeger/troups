package edu.illinois.htx.tsm;

import java.io.IOException;

public interface SharedTimestampManager extends TimestampManager {

  // note: could return SharedTimestampManager here to make it recursive
  /**
   * Creates a new shared timestamp. The timestamp has an implicit owner
   * reference and exists as long as this reference exists. I.e. if the owner
   * fails, the shared timestamp becomes subject to timestamp collection.
   *
   * @return
   * @throws IOException
   */
  long createShared() throws IOException;

  /**
   * Creates a new reference and returns the unique identifier of the reference
   * 
   * @param ts
   * @return
   * @throws IOException
   */
  long createReference(long ts) throws NoSuchTimestampException, IOException;

  /**
   * Persists the references to the shared timestamp so that the shared
   * timestamp is only deleted if all references passed to this method have
   * <emph>explicitly</emph> been released. I.e. even if the references are
   * implicitly deleted due to process crash, the shared reference will stay
   * alive. References that are not contained in the passed reference set
   * (created before or after this call) are not persisted.
   * 
   * @param ts
   * @param rids
   * @throws IOException
   */
  void persistReferences(long ts, Iterable<Long> rids) throws IOException;

  /**
   * 
   * @param ts
   * @param rid
   * @return true if the timestamp and reference existed and were successfully
   *         removed, false otherwise
   * @throws IOException
   */
  boolean releaseReference(long ts, long rid) throws IOException;

  /**
   * Adds
   * 
   * @param ts
   * @param rid
   * @param listener
   * @return
   * @throws IOException
   */
  boolean addReferenceListener(long ts, long rid, TimestampListener listener)
      throws IOException;

}