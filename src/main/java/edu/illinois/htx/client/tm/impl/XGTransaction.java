package edu.illinois.htx.client.tm.impl;

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

import edu.illinois.htx.client.tm.Transaction;
import edu.illinois.htx.tm.TransactionAbortedException;
import edu.illinois.htx.tm.region.HRegionTransactionManager;
import edu.illinois.htx.tm.region.RTM;
import edu.illinois.htx.tsm.SharedTimestampManager;
import edu.illinois.htx.tsm.TimestampManager.TimestampListener;

class XGTransaction implements Transaction, TimestampListener {

  private static enum State {
    CREATED, ACTIVE, COMMITTING, ABORTED, COMMITTED
  }

  // immutable state
  private final SharedTimestampManager stsm;
  private final ExecutorService pool;

  // mutable state
  private long id;
  private final Map<RowGroup, Long> groups = new HashMap<RowGroup, Long>();
  private State state;

  XGTransaction(SharedTimestampManager tsm, ExecutorService pool) {
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
    id = stsm.create();
    state = State.ACTIVE;
  }

  @Override
  public synchronized long enlist(HTable table, byte[] row) throws IOException {
    switch (state) {
    case ACTIVE:
      break;
    case CREATED:
    case COMMITTING:
    case ABORTED:
    case COMMITTED:
      throw newISA("enlist");
    }

    // create row group
    byte[] rootRow = HRegionTransactionManager.getSplitRow(table, row);
    RowGroup group = new RowGroup(table, rootRow);

    // this is a new row group -> enlist RTM in transaction
    if (!groups.containsKey(group)) {
      long pid = getRTM(group).join(id);
      groups.put(group, pid);
      if (!stsm.addReferenceListener(id, pid, this)) {
        rollback();
        throw new TransactionAbortedException();
      }
    }
    return id;
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
    case COMMITTED:
      return;
    case CREATED:
    case COMMITTING:
    case ABORTED:
      throw newISA("commit");
    }

    // 1. Phase: Prepare - any failure can rollback the transaction
    Throwable error = invokeAllPrepare();
    if (error != null) {
      rollback();
      if (error instanceof TransactionAbortedException)
        throw (TransactionAbortedException) error;
      throw new TransactionAbortedException(error);
    }

    // Phase 2: Commit - all participants have promised to commit
    state = State.COMMITTING;
    try {
      stsm.persistReferences(id, groups.values());
    } catch (IOException e) {
      rollback();
      throw new TransactionAbortedException(e);
    }

    /*
     * PONR: after this point the transaction is no longer allowed to fail note:
     * even if the client fails/crashes going forward, the RTMs will eventually
     * commit the transaction, since the commit is recorded in the TSM.
     */
    while (!groups.isEmpty()) {
      List<Callable<RowGroup>> commitCalls = new ArrayList<Callable<RowGroup>>(
          groups.size());
      for (final Entry<RowGroup, Long> entry : groups.entrySet()) {
        commitCalls.add(new Callable<RowGroup>() {
          @Override
          public RowGroup call() throws Exception {
            getRTM(entry.getKey()).commit(entry.getValue());
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

    // clean up timestamp (not necessary, but improves performance)
    try {
      stsm.delete(id);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * ReferenceListener implementation
   */
  @Override
  public synchronized void deleted(long rid) {
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
    for (final Entry<RowGroup, Long> entry : groups.entrySet()) {
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
    for (final Entry<RowGroup, Long> entry : groups.entrySet()) {
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
      stsm.delete(id);
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
  public String toString() {
    return id + "(" + state + ")";
  }

}
