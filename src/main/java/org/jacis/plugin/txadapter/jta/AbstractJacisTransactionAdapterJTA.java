/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.plugin.txadapter.jta;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.jacis.container.JacisContainer;
import org.jacis.container.JacisTransactionHandle;
import org.jacis.exception.JacisNoTransactionException;
import org.jacis.exception.JacisTransactionException;
import org.jacis.plugin.txadapter.JacisTransactionAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of the transaction adapter using JTA transactions.
 *
 * @author Jan Wiemer
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public abstract class AbstractJacisTransactionAdapterJTA implements JacisTransactionAdapter {

  private static final Logger log = LoggerFactory.getLogger(AbstractJacisTransactionAdapterJTA.class);

  /** Map storing the transaction handles for the active JTA transactions. */
  private final Map<Transaction, JacisTransactionHandle> transactionMap = new ConcurrentHashMap<>();
  /** Sequence to give the transaction handles a default unique id */
  private final AtomicLong txSeq = new AtomicLong(0);

  /** @return the JTA transaction manager (implementing the interface {@link TransactionManager}) */
  protected abstract TransactionManager getTransactionManager();

  /**
   * Compute a transaction ID for the transaction handle.
   * The default implementation uses the passes sequence Id.
   *
   * @param tx The external JTA transaction.
   * @param sequenceId The internally manager transaction sequence.
   * @return a transaction ID for the transaction handle.
   */
  protected String computeJacisTxId(@SuppressWarnings("UnusedParameters") Transaction tx, long sequenceId) {
    return "TX-" + sequenceId;
  }

  /**
   * Compute a transaction description for the transaction handle.
   * The default implementation uses the passes transaction id and a 'toString()' of the external transaction.
   *
   * @param tx The external JTA transaction.
   * @param txId The transaction ID for the transaction handle.
   * @return a transaction ID for the transaction handle.
   */
  protected String computeJacisTxDescription(Transaction tx, String txId) {
    return txId + "(" + String.valueOf(tx) + ")";
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

  protected Transaction getJtaTransaction() {
    try {
      TransactionManager txManager = getTransactionManager();
      Transaction tx = txManager.getTransaction();
      return tx;
    } catch (SystemException e) {
      throw new JacisTransactionException(e);
    }
  }

  protected boolean isJtaTransactionActive(Transaction tx) {
    try {
      if (tx == null) {
        return false;
      }
      int status = tx.getStatus();
      status = Status.STATUS_UNKNOWN == status ? tx.getStatus() : status; // if status unknown (transient state) -> call again
      switch (status) {
        case Status.STATUS_NO_TRANSACTION:
          return false;
        case Status.STATUS_COMMITTED:
          return transactionMap.get(tx) != null; // if state is committed we consider the Jacis TX to be still active (if there is one) since the sync committing the Jacis changes may still stand out
        case Status.STATUS_ROLLEDBACK:
          return transactionMap.get(tx) != null;
      }
      return true;
    } catch (SystemException e) {
      throw new JacisTransactionException(e);
    }
  }

  protected JacisTransactionHandle removeCurrentTransaction() {
    Transaction tx = getJtaTransaction();
    if (tx == null) {
      throw new JacisNoTransactionException("No transaction!");
    }
    JacisTransactionHandle txHandle = transactionMap.remove(tx);
    if (log.isTraceEnabled()) {
      log.trace("{} remove TX handle [{}] for JTA-Tx=[{}]. Thread: {}", this, txHandle, tx, Thread.currentThread().getName());
    }
    if (txHandle == null) {
      throw new IllegalStateException("No TX handle associated for TX" + tx);
    }
    return txHandle;
  }

  @Override
  public boolean isTransactionActive() {
    return isJtaTransactionActive(getJtaTransaction());
  }

  @Override
  public JacisTransactionHandle joinCurrentTransaction(JacisContainer container) {
    try {
      Transaction tx = getJtaTransaction();
      if (tx == null) {
        throw new JacisNoTransactionException("No transaction!");
      } else if (!isJtaTransactionActive(tx)) {
        throw new JacisNoTransactionException("No active transaction! Current Transaction: " + tx + " (state=" + tx.getStatus() + ")");
      }
      JacisTransactionHandle currentTxHandle = transactionMap.get(tx);
      if (currentTxHandle != null) {
        if (log.isTraceEnabled()) {
          log.trace("{} found existing handle [{}] for JTA-Tx=[{}]. Thread: {}", this, currentTxHandle, tx, Thread.currentThread().getName());
        }
        return currentTxHandle;
      }
      long txNr = txSeq.incrementAndGet();
      String txId = computeJacisTxId(tx, txNr);
      String txDescription = computeJacisTxDescription(tx, txId);
      JacisTransactionHandle txHandle = new JacisTransactionHandle(txId, txDescription, tx);
      tx.registerSynchronization(new JacisSync(container, txHandle));
      transactionMap.put(tx, txHandle);
      if (log.isTraceEnabled()) {
        log.trace("{} created new handle [{}] for JTA-Tx=[{}]. Thread: {}", this, currentTxHandle, tx, Thread.currentThread().getName());
      }
      return txHandle;
    } catch (SystemException | RollbackException e) {
      throw new JacisTransactionException(e);
    }
  }

  @Override
  public void disjoinCurrentTransaction() {
    removeCurrentTransaction();
  }


  /** JTA Transaction Synchronisation enlisted at the JTA transaction to let the container join the transaction. */
  protected static class JacisSync implements Synchronization {

    /** The container (type {@link JacisContainer}) that shall join the external transaction */
    private final JacisContainer container;
    /** The transaction handle (type {@link JacisTransactionHandle}) representing the external transaction inside the jacis store. */
    private final JacisTransactionHandle txHandle;


    private JacisSync(JacisContainer container, JacisTransactionHandle txHandle) {
      this.container = container;
      this.txHandle = txHandle;
    }

    @Override
    public void beforeCompletion() {
      container.internalPrepare(txHandle);
    }

    @Override
    public void afterCompletion(int status) {
      switch (status) {
        case Status.STATUS_COMMITTED:
          container.internalCommit(txHandle);
          break;
        case Status.STATUS_ROLLEDBACK:
          container.internalRollback(txHandle);
          break;
        default:
          throw new IllegalArgumentException("Illegal transaction state " + status + " after completion!");
      }
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "(" + txHandle + "@" + container + ")";
    }
  }
}
