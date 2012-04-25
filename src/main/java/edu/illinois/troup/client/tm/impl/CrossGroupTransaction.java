package edu.illinois.troup.client.tm.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.regionserver.RowGroupSplitPolicy;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;

import edu.illinois.troup.Constants;
import edu.illinois.troup.client.tm.RowGroupPolicy;
import edu.illinois.troup.client.tm.Transaction;
import edu.illinois.troup.tm.TID;
import edu.illinois.troup.tm.TransactionAbortedException;
import edu.illinois.troup.tm.XID;
import edu.illinois.troup.tm.region.HKey;
import edu.illinois.troup.tm.region.RTM;
import edu.illinois.troup.tsm.SharedTimestampManager;
import edu.illinois.troup.tsm.TimestampManager.TimestampListener;

class CrossGroupTransaction extends AbstractTransaction implements Transaction,
    TimestampListener {

  private static enum State {
    CREATED, ACTIVE, COMMITTING, ABORTED, COMMITTED
  }

  // immutable state
  private final SharedTimestampManager stsm;
  private final ExecutorService pool;

  // mutable state
  private TID id;
  private final Map<RowGroup, XID> groups = new HashMap<RowGroup, XID>();
  private final Map<String, RowGroupPolicy> strategies = new HashMap<String, RowGroupPolicy>();
  private State state;

  CrossGroupTransaction(SharedTimestampManager tsm, ExecutorService pool) {
    this.stsm = tsm;
    this.pool = pool;
    this.state = State.CREATED;
  }

  synchronized void begin() throws IOException {
    switch (state) {
    case CREATED:
      break;
    case ACTIVE:
    case COMMITTING:
    case ABORTED:
    case COMMITTED:
      throw newISA("begin");
    }
    id = new TID(stsm.acquireShared());
    state = State.ACTIVE;
  }

  @Override
  protected synchronized XID getTID(HTable table, byte[] row)
      throws IOException {
    switch (state) {
    case ACTIVE:
      break;
    case CREATED:
    case COMMITTING:
    case ABORTED:
    case COMMITTED:
      throw newISA("enlist");
    }

    // determine row group
    RowGroupPolicy strategy = getStrategy(table);
    if (strategy != null)
      row = strategy.getGroupKey(row);
    RowGroup group = new RowGroup(table, row);

    // this is a new row group -> enlist RTM in transaction
    XID xid = groups.get(group);
    if (xid == null) {
      xid = getRTM(group).join(id, new HKey(row));
      groups.put(group, xid);
      if (!stsm.addReferenceListener(xid.getTS(), xid.getPid(), this)) {
        rollback();
        throw new TransactionAbortedException();
      }
    }
    return xid;
  }

  @Override
  protected String getTIDAttr() {
    return Constants.ATTR_NAME_XID;
  }

  @Override
  public synchronized void rollback() {
    switch (state) {
    case ACTIVE:
      break;
    case ABORTED:
      return;
    case CREATED:
    case COMMITTING:
    case COMMITTED:
      throw newISA("rollback");
    }
    invokeAllAbort();
    state = State.ABORTED;
  }

  @Override
  public synchronized void commit() throws TransactionAbortedException {
    switch (state) {
    case ACTIVE:
      break;
    case COMMITTING:
    case COMMITTED:
      return;
    case CREATED:
    case ABORTED:
      throw newISA("commit");
    }

    if (groups.size() == 1)
      onePhaseCommit();
    else
      twoPhaseCommit();

    // clean up timestamp (not necessary, but improves performance)
    try {
      stsm.release(id.getTS());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void onePhaseCommit() {
    Entry<RowGroup, XID> group = groups.entrySet().iterator().next();
    try {
      getRTM(group.getKey()).commit(group.getValue(), true);
    } catch (IOException e) {
      throw new RuntimeException("Failed to commit", e);
    }
    state = State.COMMITTED;
  }

  private void twoPhaseCommit() throws TransactionAbortedException {
    // 1. Phase: Prepare - any failure can rollback the transaction
    Throwable error = invokeAllPrepare();
    if (error != null) {
      rollback();
      if (error instanceof TransactionAbortedException)
        throw (TransactionAbortedException) error;
      throw new TransactionAbortedException(error);
    }

    // Phase 2: Commit - all participants have promised to commit
    try {
      stsm.persistReferences(id.getTS(),
          Iterables.transform(groups.values(), new Function<XID, Long>() {
            @Override
            public Long apply(XID xid) {
              return xid.getPid();
            }
          }));
    } catch (IOException e) {
      rollback();
      throw new TransactionAbortedException(e);
    }
    state = State.COMMITTING;

    /*
     * PONR: after this point the transaction is no longer allowed to fail note:
     * even if the client fails/crashes going forward, the RTMs will eventually
     * commit the transaction, since the commit is recorded in the TSM.
     */
    while (!groups.isEmpty()) {
      List<Callable<RowGroup>> commitCalls = new ArrayList<Callable<RowGroup>>(
          groups.size());
      for (final Entry<RowGroup, XID> entry : groups.entrySet()) {
        commitCalls.add(new Callable<RowGroup>() {
          @Override
          public RowGroup call() throws Exception {
            getRTM(entry.getKey()).commit(entry.getValue(), false);
            return entry.getKey();
          }
        });
      }

      List<Future<RowGroup>> futures;
      while (true)
        try {
          futures = pool.invokeAll(commitCalls);
          break;
        } catch (InterruptedException e) {
          Thread.interrupted();
        }

      for (Future<RowGroup> future : futures) {
        try {
          while (true) {
            try {
              RowGroup group = future.get();
              groups.remove(group);
              break;
            } catch (InterruptedException e) {
              Thread.interrupted();
            }
          }
        } catch (ExecutionException e) {
          e.printStackTrace();
        }
      }
    }
    state = State.COMMITTED;
  }

  /**
   * ReferenceListener implementation
   */
  @Override
  public synchronized void released(long rid) {
    switch (state) {
    case ACTIVE:
      break;
    case COMMITTING:
    case COMMITTED:
    case ABORTED:
      return;
    case CREATED:
      throw newISA("deleted");
    }
    rollback();
  }

  private Throwable invokeAllPrepare() throws TransactionAbortedException {
    List<Callable<Void>> prepareCalls = new ArrayList<Callable<Void>>(
        groups.size());
    for (final Entry<RowGroup, XID> entry : groups.entrySet()) {
      prepareCalls.add(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          getRTM(entry.getKey()).prepare(entry.getValue());
          return null;
        }
      });
    }

    List<Future<Void>> futures;
    try {
      futures = pool.invokeAll(prepareCalls);
    } catch (InterruptedException e1) {
      Thread.interrupted();
      rollback();
      throw new TransactionAbortedException(e1);
    }

    Throwable error = null;
    for (Future<Void> future : futures) {
      try {
        future.get();
      } catch (InterruptedException e) {
        Thread.interrupted();
        e.printStackTrace();
        error = e;
      } catch (ExecutionException e) {
        e.printStackTrace();
        error = e.getCause();
      }
    }
    return error;
  }

  private void invokeAllAbort() {
    List<Callable<Void>> abortCalls = new ArrayList<Callable<Void>>(
        groups.size());
    for (final Entry<RowGroup, XID> entry : groups.entrySet()) {
      abortCalls.add(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          getRTM(entry.getKey()).abort(entry.getValue());
          return null;
        }
      });
    }

    List<Future<Void>> futures = null;
    try {
      futures = pool.invokeAll(abortCalls);
    } catch (InterruptedException e1) {
      Thread.interrupted();
    }

    if (futures != null)
      for (Future<Void> future : futures) {
        try {
          future.get();
        } catch (InterruptedException e) {
          Thread.interrupted();
        } catch (ExecutionException e) {
          e.printStackTrace();
        }
      }

    // clean up timestamp (not necessary, but improves performance)
    try {
      stsm.release(id.getTS());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  protected RTM getRTM(RowGroup group) {
    return group.getTable().coprocessorProxy(RTM.class, group.getRootRow());
  }

  private IllegalStateException newISA(String op) {
    return new IllegalStateException("Cannot " + op + " Transaction: " + this);
  }

  @Override
  public synchronized String toString() {
    return id.getTS() + " (" + state + ") " + groups.toString();
  }

  private RowGroupPolicy getStrategy(HTable table) throws IOException {
    byte[] bName = table.getTableName();
    String name = Bytes.toString(bName);
    RowGroupPolicy strategy = strategies.get(name);
    if (strategy == null)
      strategies.put(name,
          strategy = RowGroupSplitPolicy.getRowGroupStrategy(table));
    return strategy;
  }
}
