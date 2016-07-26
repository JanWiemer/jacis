/*
 * Copyright (c) 2016. Jan Wiemer
 */

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
class StoreTxDemarcationExecutor {

  private Logger logger = LoggerFactory.getLogger(StoreTxDemarcationExecutor.class);

  <K, TV, CV> void executePrepare(JacisStore<K, TV, CV> store, JacisTransactionHandle transaction) {
    JacisStoreTxView<K, TV, CV> txView = store.getTxView(transaction, false);
    if (txView == null) {
      return;
    } else if (txView.isReadOnly()) {
      throw new IllegalStateException("Prepare not allowed for read only transaction " + txView + "!");
    } else if (txView.isInvalidated()) {
      logger.warn("ignored prepare invalidated {} on {} (invalidated because {}) by Thread {}", txView, store, txView.getInvalidationReason(), Thread.currentThread().getName());
      return;
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

  <K, TV, CV> void executeCommit(JacisStore<K, TV, CV> store, JacisTransactionHandle transaction) {
    JacisStoreTxView<K, TV, CV> txView = store.getTxView(transaction, false);
    if (txView == null) {
      return;
    } else if (txView.isReadOnly()) {
      throw new IllegalStateException("Commit not allowed for read only transaction " + txView + "!");
    } else if (txView.isInvalidated()) {
      logger.warn("ignored internalCommit invalidated {} on {} (invalidated because {}) by Thread {}", txView, store, txView.getInvalidationReason(), Thread.currentThread().getName());
      return;
    }
    if (!txView.isCommitPending()) {
      executePrepare(store, transaction);
    }
    boolean trace = logger.isTraceEnabled();
    if (trace) {
      logger.trace("internalCommit {} on {} by Thread {}", txView, store, Thread.currentThread().getName());
    }
    for (StoreEntryTxView<K, TV, CV> entryTxView : txView.getAllEntryTxViews()) {
      K key = entryTxView.getKey();
      StoreEntry<K, TV, CV> entryCommitted = entryTxView.getCommitedEntry();
      if (entryTxView.isUpdated()) {
        if (trace) {
          logger.trace("... internalCommit {}, Store: {}", store.getObjectInfo(key), store);
        }
        trackModification(store, key, entryTxView.getOrigValue(), entryTxView.getValue(), txView.getTransaction());
        entryCommitted.update(entryTxView, txView);
      }
      entryCommitted.releaseLockedFor(txView);
      store.checkRemoveCommittedEntry(entryCommitted, txView);
    }
    txView.destroy();
  }

  <K, TV, CV> void executeRollback(JacisStore<K, TV, CV> store, JacisTransactionHandle transaction) {
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
      logger.trace("rollback {} on {} by Thread {}", txView, store, Thread.currentThread().getName());
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
    assert store.getObjectTypeSpec().isTrackOriginalValueEnabled() : "Tracking modification is only possible if original value is tracked";
    for (JacisModificationListener<K, TV> listener : store.getModificationListeners()) {
      listener.onModification(key, oldValue, newValue, tx);
    }
  }

}
