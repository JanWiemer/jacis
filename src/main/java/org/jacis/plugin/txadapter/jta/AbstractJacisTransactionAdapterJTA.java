/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.plugin.txadapter.jta;

import org.jacis.JacisApi;
import org.jacis.container.JacisContainer;
import org.jacis.container.JacisTransactionHandle;
import org.jacis.exception.JacisNoTransactionException;
import org.jacis.exception.JacisTransactionException;
import org.jacis.plugin.txadapter.JacisTransactionAdapter;
import org.jacis.store.JacisTransactionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.transaction.*;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Default implementation of the transaction adapter using JTA transactions.
 *
 * @author Jan Wiemer
 */
@SuppressWarnings({"unused", "UnusedReturnValue"}) // since this is an API of the library
@JacisApi // because it is intended to be extended
public abstract class AbstractJacisTransactionAdapterJTA implements JacisTransactionAdapter {

  private static final Logger log = LoggerFactory.getLogger(AbstractJacisTransactionAdapterJTA.class);

  /** Map storing the transaction handles for the active JTA transactions. */
  private final Map<Transaction, JacisTransactionHandle> transactionMap = new ConcurrentHashMap<>();
  /** Sequence to give the transaction handles a default unique id */
  private final AtomicLong txSeq = new AtomicLong(0);

  /** @return the JTA transaction manager (implementing the interface {@link TransactionManager}) */
  protected abstract TransactionManager getTransactionManager();

  @Override
  public JacisTransactionHandle getTransactionHandle(Object externalTransaction) {
    return transactionMap.get(externalTransaction);
  }

  @Override
  public Collection<JacisTransactionHandle> getAllTransactionHandles() {
    return transactionMap.values();
  }

  /**
   * Compute a transaction ID for the transaction handle.
   * The default implementation uses the passes sequence id.
   *
   * @param tx         The external JTA transaction.
   * @param sequenceId The internal manager transaction sequence.
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
    return txId + "(" + tx + ")";
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

  protected Transaction getJtaTransaction() {
    try {
      TransactionManager txManager = getTransactionManager();
      return txManager.getTransaction();
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

  protected JacisTransactionHandle removeCurrentTransaction(JacisTransactionHandle activeTx) {
    Transaction tx = getJtaTransaction();
    if (tx == null && activeTx != null) {
      var matchingEntry = transactionMap.entrySet().stream().filter(e -> activeTx.equals(e.getValue())).findFirst();
      if (matchingEntry.isPresent()) {
        tx = matchingEntry.get().getKey();
        transactionMap.remove(tx);
        log.trace("{} remove TX handle [{}] for no longer active JTA-Tx=[{}]. Thread: {}", this, activeTx, tx, Thread.currentThread().getName());
      }
      return activeTx;
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
      Synchronization jacisSynchronization = new JacisSync(container, txHandle);
      registerJacisSynchronization(tx, jacisSynchronization);
      transactionMap.put(tx, txHandle);
      if (log.isTraceEnabled()) {
        log.trace("{} created new handle [{}] for JTA-Tx=[{}]. Thread: {}", this, txHandle, tx, Thread.currentThread().getName());
      }
      return txHandle;
    } catch (SystemException | RollbackException e) {
      throw new JacisTransactionException(e);
    }
  }

  protected void registerJacisSynchronization(Transaction tx, Synchronization jacisSynchronization) throws RollbackException, SystemException {
    tx.registerSynchronization(jacisSynchronization);
  }

  @Override
  public void disjoinCurrentTransaction(JacisTransactionHandle activeTx) {
    removeCurrentTransaction(activeTx);
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
      StringBuilder b = new StringBuilder();
      b.append(getClass().getSimpleName());
      b.append("(").append(txHandle.getTxId());
      b.append(":");
      JacisTransactionInfo txInfo = container.getLastFinishedTransactionInfo();
      if (txInfo == null || !txInfo.getTxId().equals(txHandle.getTxId())) {
        txInfo = container.getTransactionInfo(txHandle);
      }
      if (txInfo != null) {
        //noinspection SpellCheckingInspection
        // b.append("#stores: ").append(txInfo.getStoreTxInfos().size()).append(":");
        for (JacisTransactionInfo.StoreTxInfo storeTxInfo : txInfo.getStoreTxInfos()) {
          b.append(storeTxInfo.getStoreIdentifier().getValueClass().getSimpleName());
          b.append("(#: ").append(storeTxInfo.getNumberOfTxViewEntries());
          b.append(",upd.: ").append(storeTxInfo.getNumberOfUpdatedTxViewEntries());
          b.append("), ");
        }
        b.append("took ").append(txInfo.getDurationMs()).append(" ms");
      } else {
        b.append("-noTxInfo-");
      }
      b.append(", desc: ").append(txHandle.getTxDescription());
      b.append(")");
      return b.toString();
    }
  }

}
