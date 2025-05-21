/*
 * Copyright (c) 2017. Jan Wiemer
 */

package org.jacis.store;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.jacis.JacisApi;
import org.jacis.container.JacisContainer;
import org.jacis.container.JacisContainer.StoreIdentifier;
import org.jacis.container.JacisTransactionHandle;

/**
 * Object providing some monitoring information regarding a Jacis transaction.
 *
 * @author Jan Wiemer
 */
@SuppressWarnings("unused")
@JacisApi
public class JacisTransactionInfo implements Serializable {

  private static final long serialVersionUID = 1L;

  /** The id of the transaction */
  private final String txId;
  /** Description for the transaction giving some more information about the purpose of the transaction (for logging and debugging) */
  private final String txDescription;
  /** A reference to the external (global) transaction (e.g. a JTA transaction) */
  private final Object externalTransaction;
  /** For each store participating in the transaction some monitoring data */
  @SuppressWarnings("SpellCheckingInspection")
  private final List<StoreTxInfo> storeTxInfos;
  /** Creation timestamp of the transaction in milliseconds (System.currentTimeMillis()) */
  private final long creationTimestampMs;
  /** Snapshot timestamp of the transaction in milliseconds (System.currentTimeMillis()) */
  private final long snapshotTimestampMs;

  public JacisTransactionInfo(JacisTransactionHandle txHandle, JacisContainer container, Collection<JacisStore<?, ?>> stores, long snapshotTimestampMs) {
    this.txId = txHandle.getTxId();
    this.txDescription = txHandle.getTxDescription();
    this.externalTransaction = txHandle.getExternalTransaction();
    List<StoreTxInfo> storeInfo = new ArrayList<>();
    for (JacisStore<?, ?> store : stores) {
      JacisStoreImpl<?, ?, ?> storeImpl = (JacisStoreImpl<?, ?, ?>) store;
      JacisStoreTxView<?, ?, ?> txView = storeImpl.getTxView(txHandle, false);
      if (txView != null) {
        storeInfo.add(new StoreTxInfo(storeImpl, txView));
      }
    }
    this.storeTxInfos = storeInfo;
    this.creationTimestampMs = txHandle.getCreationTimestampMs();
    this.snapshotTimestampMs = snapshotTimestampMs;
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

  /** @return for each store participating in the transaction some monitoring data */
  @SuppressWarnings("SpellCheckingInspection")
  public List<StoreTxInfo> getStoreTxInfos() {
    return storeTxInfos;
  }

  public long getCreationTimestampMs() {
    return creationTimestampMs;
  }

  public long getSnapshotTimestampMs() {
    return snapshotTimestampMs;
  }

  public long getDurationMs() {
    return snapshotTimestampMs - creationTimestampMs;
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
    JacisTransactionInfo that = (JacisTransactionInfo) obj;
    return Objects.equals(externalTransaction, that.externalTransaction);
  }

  @Override
  public String toString() {
    return "TX-INFO(" + txId + ": " + storeTxInfos + ", duration " + getDurationMs() + " ms)";
  }

  /** Transaction information regarding a certain store. */
  @JacisApi
  public static class StoreTxInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    /** The store identifier uniquely identifying this store inside the container */
    private final StoreIdentifier storeIdentifier;
    /** The id of the transaction */
    private final String txId;
    /** Description for the transaction giving some more information about the purpose of the transaction (for logging and debugging) */
    private final String txDescription;
    /** Creation timestamp in milliseconds (<code>System.currentTimeMillis()</code>) */
    private final long txCreationTimestampMs;
    /** Indicates if the transaction is a read only transaction */
    private boolean readOnly;
    /** the number of entries cloned to this TX view */
    private final int numberOfTxViewEntries;
    /** the number of updated entries of this TX view */
    private final int numberOfUpdatedTxViewEntries;
    /** flag indicating if for the transaction a commit is pending, that means a prepare has already been called */
    private final boolean commitPending;
    /** flag indicating if the transaction is already committed */
    private final boolean committed;
    /** flag indicating if the transaction is rolled back */
    private final boolean rolledBack;
    /** gives the reason (null means valid) why the tx has been invalidated. Attempts to internalCommit the tx will be ignored. */
    private String invalidationReason = null;

    StoreTxInfo(JacisStoreImpl<?, ?, ?> storeImpl, JacisStoreTxView<?, ?, ?> txView) {
      this.storeIdentifier = storeImpl.getStoreIdentifier();
      this.txId = txView.getTransaction().getTxId();
      this.txDescription = txView.getTransaction().getTxDescription();
      this.txCreationTimestampMs = txView.getCreationTimestamp();
      numberOfTxViewEntries = txView.getNumberOfEntries();
      numberOfUpdatedTxViewEntries = txView.getNumberOfUpdatedEntries();
      readOnly = txView.isReadOnly();
      commitPending = txView.isCommitPending();
      committed = txView.isCommitted();
      rolledBack = txView.isRolledBack();
      invalidationReason = txView.getInvalidationReason();
    }

    public StoreIdentifier getStoreIdentifier() {
      return storeIdentifier;
    }


    public String getTxId() {
      return txId;
    }

    public String getTxDescription() {
      return txDescription;
    }

    public long getTxCreationTimestampMs() {
      return txCreationTimestampMs;
    }

    public int getNumberOfTxViewEntries() {
      return numberOfTxViewEntries;
    }

    public int getNumberOfUpdatedTxViewEntries() {
      return numberOfUpdatedTxViewEntries;
    }

    public boolean isReadOnly() {
      return readOnly;
    }

    public boolean isCommitPending() {
      return commitPending;
    }

    public boolean isCommitted() {
      return committed;
    }

    public boolean isRolledBack() {
      return rolledBack;
    }

    public String getInvalidationReason() {
      return invalidationReason;
    }

    @Override
    public String toString() {
      StringBuilder b = new StringBuilder();
      b.append(getClass().getSimpleName()).append("(");
      b.append(getStoreIdentifier().toShortString()).append(":");
      b.append(toShortString());
      return b.toString();
    }

    public String toShortString() {
      StringBuilder b = new StringBuilder();
      b.append("TX:").append(txId).append("[").append(txDescription).append("]");
      b.append(" cloned: ").append(numberOfTxViewEntries);
      b.append(", updated ").append(numberOfUpdatedTxViewEntries);
      if (isCommitPending()) {
        b.append(" [COMMIT-PENDING]");
      }
      if (isCommitted()) {
        b.append(" [COMMITTED]");
      }
      if (isRolledBack()) {
        b.append(" [ROLLED-BACK]");
      }
      if (isReadOnly()) {
        b.append(" [READ-ONLY]");
      }
      if (getInvalidationReason() != null) {
        b.append(" [INVALIDATED because ").append(getInvalidationReason()).append("]");
      }
      return b.toString();
    }

  } // end of public static class StoreTxInfo

}
