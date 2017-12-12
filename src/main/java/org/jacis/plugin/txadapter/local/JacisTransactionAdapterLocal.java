/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.plugin.txadapter.local;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.jacis.container.JacisContainer;
import org.jacis.container.JacisTransactionHandle;
import org.jacis.exception.JacisTransactionAlreadyStartedException;
import org.jacis.plugin.txadapter.JacisTransactionAdapter;

/**
 * Default implementation of the transaction adapter using local transactions.
 *
 * @author Jan Wiemer
 */
public class JacisTransactionAdapterLocal implements JacisTransactionAdapter {

  /** Thread local to store the currently active transaction handle for the current thread. */
  protected final ThreadLocal<JacisTransactionHandle> transaction = new ThreadLocal<>();
  /** Map storing the transaction handles for the active local transactions. */
  protected final Map<JacisLocalTransaction, JacisTransactionHandle> txMap = new ConcurrentHashMap<>();
  /** Sequence to give the started local transactions a unique id */
  private final AtomicLong txSeq = new AtomicLong(0);

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

  @Override
  public JacisTransactionHandle getTransactionHandle(Object externalTransaction) {
    return txMap.get(externalTransaction);
  }

  @Override
  public Collection<JacisTransactionHandle> getAllTransactionHandles() {
    return txMap.values();
  }

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
    JacisTransactionHandle tx = transaction.get();
    if (tx != null) {
      txMap.remove(tx.getExternalTransaction());
    }
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
    txMap.put(localJacisTx, tx);
    return localJacisTx;
  }
}
