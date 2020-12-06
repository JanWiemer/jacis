/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.plugin.txadapter.local;

import org.jacis.container.JacisContainer;
import org.jacis.container.JacisTransactionHandle;
import org.jacis.exception.JacisNoTransactionException;

/**
 * Representing a local transaction.
 * If local transactions are used (the {@link JacisTransactionAdapterLocal} is registered with the container)
 * a local transaction can be started at the container (see {@link JacisContainer#beginLocalTransaction(String)}).
 * The returned instance of this class can be used to {@link #prepare()}, {@link #commit()} or {@link #rollback()}
 * the transaction.
 *
 * @author Jan Wiemer
 */
public class JacisLocalTransaction {

  /**
   * Unique id for the transaction (set with the constructor)
   */
  private final String txId;
  /** Reference to the container this local transaction belongs to (needed to commit / rollback a transaction) */
  private JacisContainer jacisContainer;
  /** The transaction handle for this local transaction. Transaction handles are used inside the store to represent local or external transactions. */
  private JacisTransactionHandle jacisTransactionHandle;
  /** A flag indicating if the transaction already has been destroyed. */
  private boolean destroyed = false;

  JacisLocalTransaction(String txId) {
    this.txId = txId;
  }

  JacisLocalTransaction associateWithJacisTransaction(JacisTransactionHandle txHandle, JacisContainer container) {
    if (!txId.equals(txHandle.getTxId())) {
      throw new IllegalArgumentException("Passed txHandle " + txHandle + " does not match the id of the local transaction " + txId);
    }
    this.jacisTransactionHandle = txHandle;
    this.jacisContainer = container;
    return this;
  }

  /** @return A description of the transaction (given when creating the transaction) */
  public String getTxDescription() {
    return jacisTransactionHandle == null ? null : jacisTransactionHandle.getTxDescription();
  }

  /**
   * Prepare the local transaction.
   * This method is calling the {@link JacisContainer#internalPrepare(JacisTransactionHandle)} method on the container
   * with the transaction handle associated with this local transaction.
   * 
   * @throws JacisNoTransactionException Thrown if this local transaction is no longer active.
   */
  public void prepare() throws JacisNoTransactionException {
    checkActive();
    jacisContainer.internalPrepare(jacisTransactionHandle);
  }

  /**
   * Commit the local transaction.
   * This method is calling the {@link JacisContainer#internalCommit(JacisTransactionHandle)} method on the container
   * with the transaction handle associated with this local transaction.
   * 
   * @throws JacisNoTransactionException Thrown if this local transaction is no longer active.
   */
  public void commit() throws JacisNoTransactionException {
    checkActive();
    jacisContainer.internalCommit(jacisTransactionHandle);
    destroy();
  }

  /**
   * Rollback the local transaction.
   * This method is calling the {@link JacisContainer#internalRollback(JacisTransactionHandle)} method on the container
   * with the transaction handle associated with this local transaction.
   * 
   * @throws JacisNoTransactionException Thrown if this local transaction is no longer active.
   */
  public void rollback() throws JacisNoTransactionException {
    checkActive();
    jacisContainer.internalRollback(jacisTransactionHandle);
    destroy();
  }

  private void checkActive() throws JacisNoTransactionException {
    if (destroyed) {
      throw new JacisNoTransactionException("Transaction already destroyed: " + this);
    }
  }

  private void destroy() {
    jacisContainer = null;
    jacisTransactionHandle = null;
    destroyed = true;
  }

  @Override
  public int hashCode() {
    return txId.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (obj == null) {
      return false;
    } else if (getClass() != obj.getClass()) {
      return false;
    }
    JacisLocalTransaction that = (JacisLocalTransaction) obj;
    return txId == null ? that.txId == null : txId.equals(that.txId);
  }

  @Override
  public String toString() {
    return "TX(" + txId + ")";
  }

}
