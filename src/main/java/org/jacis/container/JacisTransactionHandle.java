/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.container;

import org.jacis.JacisApi;

/**
 * Jacis handle for an external transaction.
 *
 * This class is used by the Jacis store to reference an external transaction the Jacis container is bound to.
 *
 * @author Jan Wiemer
 */
@JacisApi
public class JacisTransactionHandle {

  /** The id of the transaction */
  private final String txId;
  /** Description for the transaction giving some more information about the purpose of the transaction (for logging and debugging) */
  private final String txDescription;
  /** A reference to the external (global) transaction (e.g. a JTA transaction) */
  private final Object externalTransaction;
  /** Creation timestamp in milliseconds (<code>System.currentTimeMillis()</code>) */
  private final long creationTimestampMs;

  /**
   * Creates a transaction handle with the passed parameters.
   *
   * @param txId                The id of the transaction
   * @param txDescription       A description for the transaction giving some more information about the purpose of the transaction (for logging and debugging)
   * @param externalTransaction A reference to the external (global) transaction (e.g. a JTA transaction) this handle represents
   */
  public JacisTransactionHandle(String txId, String txDescription, Object externalTransaction) {
    this.txId = txId;
    this.txDescription = txDescription;
    this.externalTransaction = externalTransaction;
    this.creationTimestampMs = System.currentTimeMillis();
  }

  /** @return The id of the transaction */
  public String getTxId() {
    return txId;
  }

  /** @return A description for the transaction giving some more information about the purpose of the transaction (for logging and debugging) */
  public String getTxDescription() {
    return txDescription;
  }

  /** @return A reference to the external (global) transaction (e.g. a JTA transaction) this handle represents */
  public Object getExternalTransaction() {
    return externalTransaction;
  }

  /** @return Creation timestamp in milliseconds (System.currentTimeMillis()) */
  public long getCreationTimestampMs() {
    return creationTimestampMs;
  }

  @Override
  public int hashCode() {
    return externalTransaction.hashCode();
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
    JacisTransactionHandle that = (JacisTransactionHandle) obj;
    return externalTransaction == null ? that.externalTransaction == null : externalTransaction.equals(that.externalTransaction);
  }

  @Override
  public String toString() {
    return "TX(" + txId + ": " + txDescription + ")";
  }

}
