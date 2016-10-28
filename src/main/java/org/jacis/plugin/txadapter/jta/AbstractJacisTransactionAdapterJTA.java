/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.plugin.txadapter.jta;

import org.jacis.container.JacisContainer;
import org.jacis.container.JacisTransactionHandle;
import org.jacis.exception.JacisTransactionException;
import org.jacis.plugin.txadapter.JacisTransactionAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.transaction.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Default implementation of the transaction adapter using JTA transactions.
 *
 * @author Jan Wiemer
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public abstract class AbstractJacisTransactionAdapterJTA implements JacisTransactionAdapter {

  private static final Logger log = LoggerFactory.getLogger(AbstractJacisTransactionAdapterJTA.class);

  /** Thread local to store the currently active transaction handle for the current thread. */
  protected final ThreadLocal<JacisTransactionHandle> transaction = new ThreadLocal<>();
  /** Sequence to give the transaction handles a default unique id */
  private final AtomicLong txSeq = new AtomicLong(0);

  /** @return the JTA transaction manager (implementing the interface {@link TransactionManager}) */
  protected abstract TransactionManager getTransactionManager();

  /**
   * Compute a transaction ID for the transaction handle.
   * The default implementation uses the passes sequence Id.
   *
   * @param tx         The external JTA transaction.
   * @param sequenceId The inernally manager transaction sequence.
   * @return a transaction ID for the transaction handle.
   */
  protected String computeJacisTxId(@SuppressWarnings("UnusedParameters") Transaction tx, long sequenceId) {
    return "TX-" + sequenceId;
  }

  /**
   * Compute a transaction description for the transaction handle.
   * The default implementation uses the passes transaction id and a 'toString()' of the external transaction.
   *
   * @param tx   The external JTA transaction.
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

  @Override
  public boolean isTransactionActive() {
    try {
      TransactionManager txManager = getTransactionManager();
      return isTransactionActive(txManager.getTransaction());
    } catch (SystemException e) {
      throw new JacisTransactionException(e);
    }
  }


  @Override
  public JacisTransactionHandle joinCurrentTransaction(JacisContainer container) {
    try {
      JacisTransactionHandle currentTxHandle = transaction.get();
      TransactionManager txManager = getTransactionManager();
      Transaction tx = txManager.getTransaction();
      tx = isTransactionActive(tx) ? tx : null;
      if (tx == null) {
        if (currentTxHandle == null) {
          if (log.isTraceEnabled()) {
            log.trace("{} no transaction active, no handle stored for thread {}", this, Thread.currentThread().getName());
          }
          return null;
        } else { // currentTxHandle != null
          if (log.isTraceEnabled()) {
            log.trace("{} no transaction active, handle stored: [{}] for thread {}", this, currentTxHandle, Thread.currentThread().getName());
          }
          container.internalRollback(currentTxHandle); // transaction no longer active
          log.warn("{}: JTA transaction for transaction handle [{}] no longer active!", this, currentTxHandle);
          transaction.remove();
          return null;
        }
      } else if (currentTxHandle != null) {
        if (tx.equals(currentTxHandle.getExternalTransaction())) {
          if (log.isTraceEnabled()) {
            log.trace("{} transaction active and matching handle stored: [{}] for thread {}", this, currentTxHandle, Thread.currentThread().getName());
          }
          return currentTxHandle;
        } else {
          if (log.isTraceEnabled()) {
            log.trace("{} transaction active and not matching handle stored: [{}] (active TX: [{}]) for thread {}", this, currentTxHandle, tx, Thread.currentThread().getName());
          }
          container.internalRollback(currentTxHandle); // transaction no longer active
          log.warn("{}: active JTA transaction [{}] no longer matches the transaction handle [{}]!", this, tx, currentTxHandle);
          // do not return but continue to join the new active tx
        }
      } else {
        if (log.isTraceEnabled()) {
          log.trace("{} transaction active and no handle stored (active TX: [{}])  for thread {}", this, tx, Thread.currentThread().getName());
        }
      }
      long txNr = txSeq.incrementAndGet();
      String txId = computeJacisTxId(tx, txNr);
      String txDescription = computeJacisTxDescription(tx, txId);
      JacisTransactionHandle txHandle = new JacisTransactionHandle(txId, txDescription, tx);
      tx.registerSynchronization(new JacisSync(container, txHandle));
      transaction.set(txHandle);
      if (log.isTraceEnabled()) {
        log.trace("{} created new handle [{}] for active TX: [{}] for thread {}", this, txHandle, tx, Thread.currentThread().getName());
      }
      return txHandle;
    } catch (SystemException | RollbackException e) {
      throw new JacisTransactionException(e);
    }
  }

  @Override
  public void disjoinCurrentTransaction() {
    transaction.remove();
  }

  protected boolean isTransactionActive(Transaction tx) throws SystemException {
    if (tx == null) {
      return false;
    }
    int status = tx.getStatus();
    status = Status.STATUS_UNKNOWN == status ? tx.getStatus() : status; // if status unknown (transient state) -> call again
    switch (status) {
      case Status.STATUS_NO_TRANSACTION:
        return false;
    }
    return true;
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
