/*
 * Copyright (c) 2017. Jan Wiemer
 */

package org.jacis.store;

import java.util.concurrent.locks.ReadWriteLock;

import org.jacis.container.JacisTransactionHandle;
import org.jacis.exception.JacisModificationListenerException;
import org.jacis.exception.JacisTrackedViewModificationException;
import org.jacis.plugin.JacisModificationListener;
import org.jacis.plugin.dirtycheck.JacisDirtyCheck;
import org.jacis.plugin.persistence.JacisPersistenceAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class contains the code for actual transaction demarcation.
 *
 * @author Jan Wiemer
 */
class StoreTxDemarcationExecutor {

  private Logger logger = LoggerFactory.getLogger(StoreTxDemarcationExecutor.class);

  private <K, TV, CV> void executeDirtyCheck(JacisStoreAdminInterface<K, TV, CV> store, JacisStoreTxView<K, TV, CV> txView) {
    JacisDirtyCheck<K, TV> dirtyChecker = store.getObjectTypeSpec().getDirtyCheck();
    if (dirtyChecker == null) {
      return;
    }
    logger.trace("dirty check {} on {} by Thread {}", txView, this, Thread.currentThread().getName());
    for (StoreEntryTxView<K, TV, CV> entryTxView : txView.getAllEntryTxViews()) {
      if (!entryTxView.isUpdated()) {
        K key = entryTxView.getKey();
        TV value = entryTxView.getValue();
        TV origValue = entryTxView.getOrigValue();
        boolean dirty = dirtyChecker.isDirty(key, origValue, value);
        if (dirty) {
          logger.debug("detected dirty object not marked as updated {}", key);
          if (logger.isTraceEnabled()) {
            logger.debug(" ... orig value: {}", origValue);
            logger.debug(" ... new value : {}", value);
          }
          txView.updateValue(entryTxView, value);
        }
      }
    }
  }

  <K, TV, CV> void executePrepare(JacisStoreImpl<K, TV, CV> store, JacisTransactionHandle transaction, ReadWriteLock storeAccessLock) {
    JacisStoreTxView<K, TV, CV> txView = store.getTxView(transaction, false);
    if (txView == null) {
      return;
    } else if (txView.isReadOnly()) {
      throw new IllegalStateException("Prepare not allowed for read only transaction " + txView + "!");
    } else if (txView.isInvalidated()) {
      logger.warn("ignored prepare invalidated {} on {} (invalidated because {}) by Thread {}", txView, store, txView.getInvalidationReason(), Thread.currentThread().getName());
      return;
    }
    executeDirtyCheck(store, txView);
    logger.trace("prepare {} on {} by Thread {}", txView, this, Thread.currentThread().getName());
    txView.startCommitPhase();
    if (txView.getNumberOfUpdatedEntries() > 0) {
      try {
        storeAccessLock.writeLock().lock();
        for (StoreEntryTxView<K, TV, CV> entryTxView : txView.getAllEntryTxViews()) {
          StoreEntry<K, TV, CV> entryCommitted = entryTxView.getCommittedEntry();
          if (entryTxView.isUpdated()) {
            entryTxView.assertNotStale(txView);
            entryCommitted.lockedFor(txView);
          }
        }
        store.getIndexRegistry().lockUniqueIndexKeysForTx(txView.getTransaction());
      } finally {
        storeAccessLock.writeLock().unlock();
      }
    }
    JacisPersistenceAdapter<K, TV> persistenceAdapter = store.getObjectTypeSpec().getPersistenceAdapter();
    if (persistenceAdapter != null) {
      persistenceAdapter.afterPrepareForStore(store, transaction);
    }
  }

  <K, TV, CV> void executeCommit(JacisStoreImpl<K, TV, CV> store, JacisTransactionHandle transaction, ReadWriteLock storeAccessLock) {
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
      executePrepare(store, transaction, storeAccessLock);
    }
    boolean trace = logger.isTraceEnabled();
    if (trace) {
      logger.trace("internalCommit {} on {} by Thread {}", txView, store, Thread.currentThread().getName());
    }
    RuntimeException toThrow = null;
    try {
      if (txView.getNumberOfUpdatedEntries() > 0) {
        try {
          storeAccessLock.writeLock().lock();
          store.getIndexRegistry().unlockUniqueIndexKeysForTx(txView.getTransaction());
          for (StoreEntryTxView<K, TV, CV> entryTxView : txView.getAllEntryTxViews()) {
            K key = entryTxView.getKey();
            boolean isUpdated = entryTxView.isUpdated();
            if (!isUpdated) {
              continue;
            }
            if (trace) {
              logger.trace("... internalCommit {}, Store: {}", store.getObjectInfo(key), store);
            }
            try {
              trackModification(store, key, entryTxView.getOrigValue(), entryTxView.getValue(), txView.getTransaction());
            } catch (JacisTrackedViewModificationException e) {
              if (toThrow == null) {
                toThrow = e;
              } else {
                toThrow.addSuppressed(e);
              }
            }
            store.updateCommittedEntry(key, (k, entryCommitted) -> {
              entryCommitted.update(entryTxView, txView);
              entryCommitted.releaseLockedFor(txView);
              if (store.checkRemoveCommittedEntry(entryCommitted, txView)) {
                return null;
              }
              return entryCommitted;
            });
          }
        } finally {
          storeAccessLock.writeLock().unlock();
        }
      }
      for (StoreEntryTxView<K, TV, CV> entryTxView : txView.getAllEntryTxViews()) {
        if (entryTxView.isUpdated()) {
          continue; // handled in the loop above
        }
        K key = entryTxView.getKey();
        store.updateCommittedEntry(key, (k, entryCommitted) -> {
          if (store.checkRemoveCommittedEntry(entryCommitted, txView)) {
            return null;
          }
          return entryCommitted;
        });
      }

    } finally { // even if exceptions occur TX view has to be destroyed! See https://github.com/JanWiemer/jacis/issues/8
      txView.afterCommit();
      JacisPersistenceAdapter<K, TV> persistenceAdapter = store.getObjectTypeSpec().getPersistenceAdapter();
      if (persistenceAdapter != null) {
        persistenceAdapter.afterCommitForStore(store, transaction);
      }
    }
    if (toThrow != null) {
      throw toThrow;
    }
  }

  <K, TV, CV> void executeRollback(JacisStoreImpl<K, TV, CV> store, JacisTransactionHandle transaction, ReadWriteLock storeAccessLock) {
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
    if (txView.getNumberOfUpdatedEntries() > 0) {
      try {
        storeAccessLock.writeLock().lock();
        store.getIndexRegistry().unlockUniqueIndexKeysForTx(txView.getTransaction());
        for (StoreEntryTxView<K, TV, CV> entryTxView : txView.getAllEntryTxViews()) {
          K key = entryTxView.getKey();
          if (trace) {
            logger.trace("... rollback {}, Store: {}", store.getObjectInfo(key), this);
          }
          store.updateCommittedEntry(key, (k, entryCommitted) -> {
            entryCommitted.releaseLockedFor(txView);
            if (store.checkRemoveCommittedEntry(entryCommitted, txView)) {
              return null;
            }
            return entryCommitted;
          });
        }
      } finally {
        storeAccessLock.writeLock().unlock();
      }
    }
    txView.afterRollback();
    JacisPersistenceAdapter<K, TV> persistenceAdapter = store.getObjectTypeSpec().getPersistenceAdapter();
    if (persistenceAdapter != null) {
      persistenceAdapter.afterRollbackForStore(store, transaction);
    }
  }

  <K, TV, CV> void executeDestroy(JacisStoreImpl<K, TV, CV> store, JacisTransactionHandle transaction, ReadWriteLock storeAccessLock) {
    JacisStoreTxView<K, TV, CV> txView = store.getTxView(transaction, false);
    if (txView != null) {
      txView.destroy();
    }
  }

  private <K, TV, CV> void trackModification(JacisStoreImpl<K, TV, CV> store, K key, TV oldValue, TV newValue, JacisTransactionHandle tx) {
    assert store.getObjectTypeSpec().isTrackOriginalValueEnabled() : "Tracking modification is only possible if original value is tracked";
    RuntimeException toThrow = null;
    for (JacisModificationListener<K, TV> listener : store.getModificationListeners()) {
      RuntimeException modificationException = null;
      try {
        listener.onModification(key, oldValue, newValue, tx);
      } catch (JacisTrackedViewModificationException e) {
        modificationException = e;
      } catch (Exception e) {
        modificationException = new JacisModificationListenerException(store, listener, tx, key, oldValue, newValue, e);
      }
      if (modificationException != null) {
        if (toThrow == null) {
          toThrow = modificationException;
        } else {
          toThrow.addSuppressed(modificationException);
        }
      }
    }
    if (toThrow != null) {
      throw toThrow;
    }
  }

}
