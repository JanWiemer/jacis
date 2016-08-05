/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.plugin.txadapter.local;

import org.jacis.container.JacisContainer;
import org.jacis.container.JacisTransactionHandle;
import org.jacis.exception.JacisTransactionAlreadyStartedException;
import org.jacis.plugin.txadapter.JacisTransactionAdapter;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Default implementation of the transaction adapter using local transactions.
 *
 * @author Jan Wiemer
 */
public class JacisTransactionAdapterLocal implements JacisTransactionAdapter {

  /**
   * Thread local to store the currently active transaction handle for the current thread.
   */
  protected final ThreadLocal<JacisTransactionHandle> transaction = new ThreadLocal<>();
  /** Sequence to give the started local transactions a unique id */
  private final AtomicLong txSeq = new AtomicLong(0);

  @Override
  public boolean isTransactionActive() {
    return transaction.get() != null;
  }

  @Override
  public JacisTransactionHandle joinCurrentTransaction(JacisContainer container) {
    return transaction.get();// no special operation to join needed for locally managed transactions
  }

  @Override
  public void disjoinCurrentTransaction() {
    transaction.remove();
  }

  public JacisLocalTransaction startLocalTransaction(JacisContainer jacisContainer, String txDescription) {
    JacisTransactionHandle tx = transaction.get();
    if (tx != null) {
      throw new JacisTransactionAlreadyStartedException("Transaction already started: " + tx);
    }
    long txNr = txSeq.incrementAndGet();
    String txId = "TX-" + txNr;
    JacisLocalTransaction localJacisTx = new JacisLocalTransaction(txId);
    tx = new JacisTransactionHandle(txId, txDescription, localJacisTx);
    transaction.set(tx);
    localJacisTx.associateWithJacisTransaction(tx, jacisContainer);
    return localJacisTx;
  }

}
