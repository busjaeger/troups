package edu.illinois.troups.tsm;

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
  long acquireShared() throws IOException;

  /**
   * Creates a new reference and returns the unique identifier of the reference
   * 
   * @param ts
   * @return
   * @throws IOException
   */
  long acquireReference(long ts) throws NoSuchTimestampException, IOException;

  /**
   * 
   * @param ts
   * @param rid
   * @return true if the timestamp and reference existed and were successfully
   *         removed, false otherwise
   * @throws IOException
   */
  boolean releaseReference(long ts, long rid) throws IOException;

//  boolean isReferenceHeldByMe(long ts, long rid)
//      throws NoSuchTimestampException, IOException;

  boolean isReferencePersisted(long ts, long rid)
      throws NoSuchTimestampException, IOException;

  /**
   * Must be called by owner. Persists the references to the shared timestamp so
   * that the shared timestamp is only deleted if all references passed to this
   * method have <emph>explicitly</emph> been released. I.e. even if the
   * references are implicitly deleted due to process crash, the shared
   * reference will stay alive. Owner crashes are also tolerated after calling
   * this method. References that are not contained in the passed reference set
   * (created before or after this call) are not persisted.
   * 
   * @param ts
   * @param rids
   * @throws NotOwnerException
   *           if the calling thread is the not the owner of the given timestamp
   * @throws IOException
   */
  void persistReferences(long ts, Iterable<Long> rids)
      throws NotOwnerException, IOException;

//  /**
//   * Adds
//   * 
//   * @param ts
//   * @param rid
//   * @param listener
//   * @return
//   * @throws IOException
//   */
//  boolean addReferenceListener(long ts, long rid, TimestampListener listener)
//      throws IOException;

}