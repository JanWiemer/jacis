package org.jacis.store;

import org.jacis.container.JacisTransactionHandle;
import org.jacis.plugin.JacisModificationListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Jan Wiemer
 * 
 * This class contains the code for actual transaction demarcation.
 *
 */
public class StoreTxDemarcationExecutor {

  Logger logger = LoggerFactory.getLogger(StoreTxDemarcationExecutor.class);

  public <K, TV, CV> void executePrepare(JacisStore<K, TV, CV> store, JacisTransactionHandle transaction) {
    JacisStoreTxView<K, TV, CV> txView = store.getTxView(transaction, false);
    if (txView == null) {
      return;
    } else if (txView.isReadOnly()) {
      throw new IllegalStateException("Prepare not allowed for read only transaction " + txView + "!");
    }
    logger.trace("prepare {} on {} by Thread {}", txView, this, Thread.currentThread().getName());
    txView.startCommitPhase();
    for (StoreEntryTxView<K, TV, CV> entryTxView : txView.getAllEntryTxViews()) {
      StoreEntry<K, TV, CV> entryCommitted = entryTxView.getCommitedEntry();
      if (entryTxView.isUpdated()) {
        entryTxView.assertNotStale(txView);
        entryCommitted.lockedFor(txView);
      }
    }
  }

  public <K, TV, CV> void executeCommit(JacisStore<K, TV, CV> store, JacisTransactionHandle transaction) {
    JacisStoreTxView<K, TV, CV> txView = store.getTxView(transaction, false);
    if (txView == null) {
      return;
    } else if (txView.isReadOnly()) {
      throw new IllegalStateException("Commit not allowed for read only transaction " + txView + "!");
    }
    if (!txView.isCommitPending()) {
      executePrepare(store, transaction);
    }
    boolean trace = logger.isTraceEnabled();
    if (trace) {
      logger.trace("commit {} on {} by Thread {}", txView, this, Thread.currentThread().getName());
    }
    for (StoreEntryTxView<K, TV, CV> entryTxView : txView.getAllEntryTxViews()) {
      K key = entryTxView.getKey();
      StoreEntry<K, TV, CV> entryCommitted = entryTxView.getCommitedEntry();
      if (entryTxView.isUpdated()) {
        if (trace) {
          logger.trace("... commit {}, Store: {}", store.getObjectInfo(key), store);
        }
        trackModification(store, key, entryTxView.getOrigValue(), entryTxView.getValue(), txView.getTransaction());
        entryCommitted.update(entryTxView, txView.getTxName());
      }
      entryCommitted.releaseLockedFor(txView);
      store.checkRemoveCommittedEntry(entryCommitted, txView);
    }
    txView.destroy();
  }

  public <K, TV, CV> void executeRollback(JacisStore<K, TV, CV> store, JacisTransactionHandle transaction) {
    JacisStoreTxView<K, TV, CV> txView = store.getTxView(transaction, false);
    if (txView == null) {
      return;
    }
    boolean trace = logger.isTraceEnabled();
    if (txView.isReadOnly()) {
      logger.trace("Transaction " + txView + " is marked as readonly, no rollback necessary.");
      return;
    }
    if (trace) {
      logger.trace("rollback {} on {} by Thread {}", txView, this, Thread.currentThread().getName());
    }
    for (StoreEntryTxView<K, TV, CV> entryTxView : txView.getAllEntryTxViews()) {
      K key = entryTxView.getKey();
      StoreEntry<K, TV, CV> entryCommitted = entryTxView.getCommitedEntry();
      if (trace) {
        logger.trace("... rollback {}, Store: {}", store.getObjectInfo(key), this);
      }
      entryCommitted.releaseLockedFor(txView);
      store.checkRemoveCommittedEntry(entryCommitted, txView);
    }
    txView.destroy();
  }

  private <K, TV, CV> void trackModification(JacisStore<K, TV, CV> store, K key, TV oldValue, TV newValue, JacisTransactionHandle tx) {
    for (JacisModificationListener<K, TV> listener : store.getModificationListeners()) {
      listener.onModification(key, oldValue, newValue, tx);
    }
  }

}
