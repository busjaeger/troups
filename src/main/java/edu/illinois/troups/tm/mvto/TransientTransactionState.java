package edu.illinois.troups.tm.mvto;

import edu.illinois.troups.tm.TransactionState;

interface TransientTransactionState extends TransactionState {
  public static final int BLOCKED = 8;
}