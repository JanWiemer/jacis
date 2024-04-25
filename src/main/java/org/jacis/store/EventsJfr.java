package org.jacis.store;

import jdk.jfr.*;
import org.jacis.container.JacisTransactionHandle;


public interface EventsJfr {

  @Name("org.jacis.StoreStatistic")
  @Label("Store Statistic")
  @Category({"JACIS"})
  @Description("Statistic regarding a JACIS store")
  @Period("10 s")
  @StackTrace(false)
  public class JacisStoreStatisticJfrEvent extends Event {

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

    public JacisStoreStatisticJfrEvent(JacisStoreImpl<?, ?, ?> store) {
      if (isEnabled()) {
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

  //===========================================================================================================================

  @Name("org.jacis.Transaction")
  @Label("Transaction")
  @Category({"JACIS"})
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
      super.begin();
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

  //===========================================================================================================================

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
      super.begin();
      if (isEnabled()) {
        storeId = store.getStoreIdentifier().toShortString();
        txId = transaction.getTxId();
        txDescription = transaction.getTxDescription();
        numberOfClonedEntries = txView.getNumberOfEntries();
        numberOfUpdatedEntries = txView.getNumberOfUpdatedEntries();
        readOnly = txView.isReadOnly();
        storeTxLifetime = System.currentTimeMillis() - txView.getCreationTimestamp();
      }
      return (T) this;
    }

    JacisJfrEvent setException(Throwable exception) {
      this.exception = exception == null ? null : String.valueOf(exception);
      return this;
    }
  }


  //===========================================================================================================================
  public enum OperationType {PREPARE, COMMIT, ROLLBACK}

  @Name("org.jacis.tx.StoreTransaction")
  @Label("Prepare / Commit / Rollback")
  @Category({"JACIS"})
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

  //===========================================================================================================================

  @Name("org.jacis.tx.DirtyCheck")
  @Label("Dirty-Check")
  @Category({"JACIS"})
  @Description("The dirty check for a transaction on a Jacis Store")
  @StackTrace(false)
  class JacisDirtyCheckJfrEvent extends JacisJfrEvent {
    @Label("Number Of Dirty")
    int numberOfDirty;

    public void setFoundDirty(int numberOfDirty) {
      this.numberOfDirty = numberOfDirty;
    }
  }

  //===========================================================================================================================

  @Name("org.jacis.PersistenceAdapter")
  @Label("Persistence Adapter")
  @Category({"JACIS"})
  @Description("Actions of the persistence adapter on prepare, commit or rollback for a transaction on a Jacis Store")
  @StackTrace(false)
  class PersistenceAdapterJfrEvent extends JacisTxJfrEvent {
    PersistenceAdapterJfrEvent begin(JacisTxJfrEvent src) {
      super.begin(); // do not call special begin(...) method of JacisTxJfrEvent but set the inherited properties in this method:
      if (isEnabled()) {
        operationType = src.operationType;
        storeId = src.storeId;
        txId = src.txId;
        txDescription = src.txDescription;
        numberOfClonedEntries = src.numberOfClonedEntries;
        numberOfUpdatedEntries = src.numberOfUpdatedEntries;
        readOnly = src.readOnly;
        storeTxLifetime = src.storeTxLifetime;
      }
      return this;
    }
  }

  static void withPersistentAdapterEvent(JacisTxJfrEvent surroundingEvt, Runnable task) {
    PersistenceAdapterJfrEvent jfrEvent = new EventsJfr.PersistenceAdapterJfrEvent().begin(surroundingEvt);
    Throwable exception = null;
    try {
      task.run();
      Thread.sleep(10);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      exception = e;
      throw e;
    } finally {
      jfrEvent.setException(exception).commit();
    }
  }
}
