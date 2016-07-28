/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.plugin.txadapter.local;

import org.jacis.container.JacisContainer;
import org.jacis.container.JacisTransactionHandle;
import org.jacis.exception.JacisNoTransactionException;

/**
 * @author Jan Wiemer
 *
 * Implementation of a local transaction.
 */
public class JacisLocalTransaction {

  private final String txId;
  private JacisContainer jacisContainer;
  private JacisTransactionHandle jacisTransactionHandle;
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

  public String getTxDescription() {
    return jacisTransactionHandle == null ? null : jacisTransactionHandle.getTxDescription();
  }


  public void prepare() throws JacisNoTransactionException {
    checkActive();
    jacisContainer.internalPrepare(jacisTransactionHandle);
  }

  public void commit() throws JacisNoTransactionException {
    checkActive();
    jacisContainer.internalCommit(jacisTransactionHandle);
    destroy();
  }

  public void rollback() throws JacisNoTransactionException {
    checkActive();
    jacisContainer.internalRollback(jacisTransactionHandle);
    destroy();
  }

  private void checkActive() {
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
