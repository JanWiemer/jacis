package org.jacis.store;

import jdk.jfr.*;
import org.jacis.container.JacisTransactionHandle;


public interface EventsJfr {

  @Label("org.jacis.JacisContainerTx")
  @Category({"JACIS", "TX"})
  @Description("Transaction on a JACIS container")
  @StackTrace(false)
  class JacisContainerTxJfrEvent extends Event {
    @Label("TX")
    String txId;
    @Label("TX Description")
    String txDescription;
    @BooleanFlag
    @Label("Committed")
    boolean committed;
    @Label("Number of stores")
    int numberOfStores;

    public JacisContainerTxJfrEvent(String txId, String txDescription) {
      this.txId = txId;
      this.txDescription = txDescription;
    }

    public void trackCommit(int numberOfStores) {
      this.numberOfStores = numberOfStores;
      this.committed = true;
      commit();
    }

    public void trackRollback(int numberOfStores) {
      this.numberOfStores = numberOfStores;
      this.committed = false;
      commit();
    }
  }

  @Label("org.jacis.StoreStatistic")
  @Category({"JACIS", "Statistic"})
  @Description("Statistic regarding a JACIS store")
  @Period("10 s")
  @StackTrace(false)
  class JacisStoreStatisticJfrEvent extends Event {

    @Label("Store Identifier")
    String storeId;
    @Label("Size")
    int size;
    @Label("Active Transactions")
    int numberOfActiveTxViews;
    @Label("Unique Indices")
    int numberOfUniqueIndices;
    @Label("Non-Unique Indices")
    int numberOfNonUniqueIndices;
    @Label("Non-Unique Multi-Indices")
    int numberOfNonUniqueMultiIndices;
    @Label("Modification Listeners")
    int numberOfModificationListeners;
    @Label("Tracked Views")
    int numberOfTrackedViews;

    void track(JacisStoreImpl<?, ?, ?> store) {
      if (isEnabled()) {
        begin();
        storeId = store.getStoreIdentifier().toShortString();
        size = store.size();
        numberOfActiveTxViews = store.getNumberOfActivTxViews();
        numberOfUniqueIndices = store.getIndexRegistry().getNumberOfUniqueIndices();
        numberOfNonUniqueIndices = store.getIndexRegistry().getNumberOfNonUniqueIndices();
        numberOfNonUniqueMultiIndices = store.getIndexRegistry().getNumberOfNonUniqueMultiIndices();
        numberOfModificationListeners = store.getModificationListeners().size();
        numberOfTrackedViews = store.getTrackedViewRegistry().getAllViewNames().size();
        commit();
      }
    }

  }

  abstract class JacisJfrEvent extends Event {

    @Label("Store Identifier")
    String storeId;
    @Label("TX")
    String txId;
    @Label("TX Description")
    String txDescription;
    @Label("Exception")
    String exception;
    @Label("Cloned Entries")
    int numberOfClonedEntries;
    @Label("Updated Entries")
    int numberOfUpdatedEntries;
    @BooleanFlag
    @Label("ReadOnly")
    boolean readOnly;
    @Timespan
    @Label("TX lifetime")
    long storeTxLifetime;

    <T extends JacisJfrEvent> T begin(JacisStoreAdminInterface<?, ?, ?> store, JacisStoreTxView<?, ?, ?> txView, JacisTransactionHandle transaction) {
      if (isEnabled()) {
        storeId = store.getStoreIdentifier().toShortString();
        txId = transaction.getTxId();
        txDescription = transaction.getTxDescription();
        numberOfClonedEntries = txView.getNumberOfEntries();
        numberOfUpdatedEntries = txView.getNumberOfUpdatedEntries();
        readOnly = txView.isReadOnly();
        storeTxLifetime = System.currentTimeMillis() - txView.getCreationTimestamp();
        super.begin();
      }
      return (T) this;
    }

    JacisJfrEvent setException(Throwable exception) {
      this.exception = exception == null ? null : String.valueOf(exception);
      return this;
    }

  }

  public enum OperationType {PREPARE, COMMIT, ROLLBACK}

  @Label("org.jacis.tx.Demarcation")
  @Category({"JACIS", "TX"})
  @Description("Represents a prepare, commit or rollback for a transaction on a Jacis Store")
  @StackTrace(false)
  class JacisTxJfrEvent extends JacisJfrEvent {
    @Label("Operation")
    String operationType;

    <T extends JacisTxJfrEvent> T begin(OperationType opType, JacisStoreAdminInterface<?, ?, ?> store, JacisStoreTxView<?, ?, ?> txView, JacisTransactionHandle transaction) {
      operationType = opType.name();
      return super.begin(store, txView, transaction);
    }

  }


  @Label("org.jacis.PersistenceAdapter")
  @Category({"JACIS", "Persistence"})
  @Description("Actions of the persistence adapter on prepare, commit or rollback for a transaction on a Jacis Store")
  @StackTrace(false)
  class PersistenceAdapterJfrEvent extends JacisTxJfrEvent {
    PersistenceAdapterJfrEvent begin(JacisTxJfrEvent src) {
      if (isEnabled()) {
        operationType = src.operationType;
        storeId = src.storeId;
        txId = src.txId;
        txDescription = src.txDescription;
        numberOfClonedEntries = src.numberOfClonedEntries;
        numberOfUpdatedEntries = src.numberOfUpdatedEntries;
        readOnly = src.readOnly;
        storeTxLifetime = src.storeTxLifetime;
        super.begin();
      }
      return this;
    }
  }

  @Label("org.jacis.tx.DirtyCheck")
  @Category({"JACIS", "TX"})
  @Description("The dirty check for a transaction on a Jacis Store")
  @StackTrace(false)
  class JacisDirtyCheckJfrEvent extends JacisJfrEvent {
    @Label("Number Of Dirty")
    int numberOfDirty;

    public void setFoundDirty(int numberOfDirty) {
      this.numberOfDirty = numberOfDirty;
    }
  }


  static void withPersistentAdapterEvent(JacisTxJfrEvent surroundingEvt, Runnable task) {
    PersistenceAdapterJfrEvent jfrEvent = new EventsJfr.PersistenceAdapterJfrEvent().begin(surroundingEvt);
    Throwable exception = null;
    try {
      task.run();
    } catch (Exception e) {
      exception = e;
      throw e;
    } finally {
      jfrEvent.setException(exception).commit();
    }
  }
}
