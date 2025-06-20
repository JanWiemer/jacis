/*
 * Copyright (c) 2017. Jan Wiemer
 */

package org.jacis.store;

import org.jacis.container.JacisTransactionHandle;
import org.jacis.exception.JacisModificationListenerException;
import org.jacis.exception.JacisTrackedViewModificationException;
import org.jacis.plugin.JacisModificationListener;
import org.jacis.plugin.dirtycheck.JacisDirtyCheck;
import org.jacis.plugin.persistence.JacisPersistenceAdapter;
import org.jacis.plugin.readonly.object.JacisReadonlyModeSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * This class contains the code for actual transaction demarcation.
 *
 * @author Jan Wiemer
 */
class StoreTxDemarcationExecutor {

  private final Logger logger = LoggerFactory.getLogger(StoreTxDemarcationExecutor.class);

  private <K, TV, CV> void executeDirtyCheck(JacisStoreAdminInterface<K, TV, CV> store, JacisStoreTxView<K, TV, CV> txView) {
    JacisDirtyCheck<K, TV> dirtyChecker = store.getObjectTypeSpec().getDirtyCheck();
    if (dirtyChecker == null) {
      return;
    }
    EventsJfr.JacisDirtyCheckJfrEvent jfrEvent = new EventsJfr.JacisDirtyCheckJfrEvent(store, txView, txView.getTransaction());
    jfrEvent.begin();
    Throwable exception = null;
    try {
      int foundDirty = 0;
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
            foundDirty++;
            txView.updateValue(entryTxView, value);
          }
        }
      }
      jfrEvent.setFoundDirty(foundDirty);
    } catch (Exception e) {
      exception = e;
      throw e;
    } finally {
      jfrEvent.setException(exception).commit();
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
    EventsJfr.JacisTxJfrEvent jfrEvent = new EventsJfr.JacisTxJfrEvent(EventsJfr.OperationType.PREPARE, store, txView, transaction);
    jfrEvent.begin();
    Throwable exception = null;
    try {
      executeDirtyCheck(store, txView);
      logger.trace("prepare {} on {} by Thread {}", txView, this, Thread.currentThread().getName());
      txView.startCommitPhase();
      Map<K, Long> optimisticLockVersionMap = txView.getOptimisticLockVersionMap();
      if (txView.getNumberOfUpdatedEntries() > 0 || optimisticLockVersionMap != null) {
        try {
          storeAccessLock.writeLock().lock();
          if (optimisticLockVersionMap != null) {
            for (Entry<K, Long> optLock : optimisticLockVersionMap.entrySet()) {
              Long lockedVersion = optLock.getValue();
              StoreEntry<K, TV, CV> entryCommitted = store.getCommittedEntry(optLock.getKey());
              StoreEntryTxView<K, TV, CV> entryTxView = new StoreEntryTxView<>(entryCommitted, lockedVersion);
              entryTxView.assertNotStale(txView);
              entryCommitted.lockedFor(txView);
            }
          }
          for (StoreEntryTxView<K, TV, CV> entryTxView : txView.getUpdatedEntriesForCommit()) {
            K key = entryTxView.getKey();
            if (entryTxView.getValue() != null) {
              trackBeforePrepareModification(store, key, entryTxView.getOrigValue(), entryTxView.getValue(), txView.getTransaction());
            }
            StoreEntry<K, TV, CV> entryCommitted = entryTxView.getCommittedEntry();
            entryTxView.assertNotStale(txView);
            entryCommitted.lockedFor(txView);
            if (entryTxView.getValue() != null //
                && entryTxView.getValue() instanceof JacisReadonlyModeSupport //
                && store.getObjectTypeSpec().isSwitchToReadOnlyModeInPrepare()) {
              ((JacisReadonlyModeSupport) entryTxView.getValue()).switchToReadOnlyMode();
            }
            trackPrepareModification(store, key, entryTxView.getOrigValue(), entryTxView.getValue(), txView.getTransaction());
          }
          store.getIndexRegistry().lockUniqueIndexKeysForTx(txView.getTransaction());
        } finally {
          storeAccessLock.writeLock().unlock();
        }
      }
      JacisPersistenceAdapter<K, TV> persistenceAdapter = store.getObjectTypeSpec().getPersistenceAdapter();
      if (persistenceAdapter != null) {
        EventsJfr.withPersistentAdapterEvent(jfrEvent, () -> persistenceAdapter.afterPrepareForStore(store, transaction));
      }
    } catch (Exception e) {
      exception = e;
      throw e;
    } finally {
      jfrEvent.setException(exception).commit();
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
    EventsJfr.JacisTxJfrEvent jfrEvent = new EventsJfr.JacisTxJfrEvent(EventsJfr.OperationType.COMMIT, store, txView, transaction);
    jfrEvent.begin();
    RuntimeException toThrow = null;
    Map<K, Long> optimisticLockVersionMap = txView.getOptimisticLockVersionMap();
    try {
      if (txView.getNumberOfUpdatedEntries() > 0 || optimisticLockVersionMap != null) {
        Collection<StoreEntryTxView<K, TV, CV>> updatedEntries = txView.getUpdatedEntriesForCommit();
        try {
          storeAccessLock.writeLock().lock();
          store.getIndexRegistry().unlockUniqueIndexKeysForTx(txView.getTransaction());
          // ---------- UPDATE COMMITTED ENTRIES -----------------
          for (StoreEntryTxView<K, TV, CV> entryTxView : updatedEntries) {
            K key = entryTxView.getKey();
            if (trace) {
              logger.trace("... internalCommit {}, Store: {}", store.getObjectInfo(key), store);
            }
            store.updateCommittedEntry(key, (k, entryCommitted) -> {
              entryCommitted.update(entryTxView, txView);
              entryCommitted.releaseLockedFor(txView);
              return entryCommitted;
            });
          }
          // ---------- RELEASE LOCKS FOR OPTIMISTIC LOCKS -----------------
          if (optimisticLockVersionMap != null) {
            for (K lockedKey : optimisticLockVersionMap.keySet()) {
              StoreEntry<K, TV, CV> entryCommitted = store.getCommittedEntry(lockedKey);
              if (entryCommitted.isLocked()) {
                entryCommitted.releaseLockedFor(txView);
              }
            }
          }
          // ---------- TRACK MODIFICATIONS -----------------
          for (StoreEntryTxView<K, TV, CV> entryTxView : updatedEntries) {
            K key = entryTxView.getKey();
            try {
              trackModification(store, key, entryTxView.getOrigValue(), entryTxView.getValue(), txView.getTransaction());
            } catch (JacisTrackedViewModificationException e) {
              if (toThrow == null) {
                toThrow = e;
              } else {
                toThrow.addSuppressed(e);
              }
            }
          }
        } finally {
          storeAccessLock.writeLock().unlock();
        }
      }
      store.checkRemoveCommittedEntries(txView);
    } finally { // even if exceptions occur TX view has to be destroyed! See https://github.com/JanWiemer/jacis/issues/8
      txView.afterCommit();
      JacisPersistenceAdapter<K, TV> persistenceAdapter = store.getObjectTypeSpec().getPersistenceAdapter();
      if (persistenceAdapter != null) {
        EventsJfr.withPersistentAdapterEvent(jfrEvent, () -> persistenceAdapter.afterCommitForStore(store, transaction));
      }
      jfrEvent.setException(toThrow).commit();
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
    EventsJfr.JacisTxJfrEvent jfrEvent = new EventsJfr.JacisTxJfrEvent(EventsJfr.OperationType.ROLLBACK, store, txView, transaction);
    jfrEvent.begin();
    Throwable exception = null;
    Map<K, Long> optimisticLockVersionMap = txView.getOptimisticLockVersionMap();
    try {
      if (txView.getNumberOfUpdatedEntries() > 0 || optimisticLockVersionMap != null) {
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
              return entryCommitted;
            });
          }
          // ---------- RELEASE LOCKS FOR OPTIMISTIC LOCKS -----------------
          if (optimisticLockVersionMap != null) {
            for (K lockedKey : optimisticLockVersionMap.keySet()) {
              StoreEntry<K, TV, CV> entryCommitted = store.getCommittedEntry(lockedKey);
              if (entryCommitted.isLocked()) {
                entryCommitted.releaseLockedFor(txView);
              }
            }
          }
        } finally {
          storeAccessLock.writeLock().unlock();
        }
      }
      store.checkRemoveCommittedEntries(txView);
      txView.afterRollback();
      JacisPersistenceAdapter<K, TV> persistenceAdapter = store.getObjectTypeSpec().getPersistenceAdapter();
      if (persistenceAdapter != null) {
        EventsJfr.withPersistentAdapterEvent(jfrEvent, () -> persistenceAdapter.afterRollbackForStore(store, transaction));
      }
    } catch (Exception e) {
      exception = e;
      throw e;
    } finally {
      jfrEvent.setException(exception).commit();
    }
  }

  <K, TV, CV> void executeDestroy(JacisStoreImpl<K, TV, CV> store, JacisTransactionHandle transaction, ReadWriteLock storeAccessLock) {
    JacisStoreTxView<K, TV, CV> txView = store.getTxView(transaction, false);
    if (txView != null) {
      txView.destroy();
    }
  }

  private <K, TV, CV> void trackBeforePrepareModification(JacisStoreImpl<K, TV, CV> store, K key, TV oldValue, TV newValue, JacisTransactionHandle tx) {
    assert store.getObjectTypeSpec().isTrackOriginalValueEnabled() : "Tracking prepared modification is only possible if original value is tracked";
    for (JacisModificationListener<K, TV> listener : store.getModificationListeners()) {
      listener.onAdjustBeforePrepare(key, oldValue, newValue, tx);
    }
  }

  private <K, TV, CV> void trackPrepareModification(JacisStoreImpl<K, TV, CV> store, K key, TV oldValue, TV newValue, JacisTransactionHandle tx) {
    assert store.getObjectTypeSpec().isTrackOriginalValueEnabled() : "Tracking prepared modification is only possible if original value is tracked";
    for (JacisModificationListener<K, TV> listener : store.getModificationListeners()) {
      listener.onPrepareModification(key, oldValue, newValue, tx);
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
