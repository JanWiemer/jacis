/*
 * Copyright (c) 2017. Jan Wiemer
 */

package org.jacis.store;

import org.jacis.container.JacisContainer;
import org.jacis.container.JacisContainer.StoreIdentifier;
import org.jacis.container.JacisObjectTypeSpec;
import org.jacis.container.JacisTransactionHandle;
import org.jacis.exception.JacisStaleObjectException;
import org.jacis.exception.JacisTransactionAlreadyPreparedForCommitException;
import org.jacis.index.*;
import org.jacis.plugin.JacisModificationListener;
import org.jacis.plugin.objectadapter.JacisObjectAdapter;
import org.jacis.util.ConcurrentWeakHashMap;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Storing a single type of objects.
 * <p>
 * All operations checking or returning entries of the store operate on the committed values merged with the
 * current transactional view (obtained with the currently active transaction handle from the map {@link #txViewMap}).
 * This means that first the transactional view is checked if it contains an entry for the desired key.
 * If so this entry is returned, otherwise the committed value from the core store (see {@link #store}) is returned.
 * Note that if an object is deleted in a transaction an entry with the value <code>null</code> remains in the transactional view.
 * Therefore, also deletions are properly handled with respect to isolation.
 *
 * @param <K>  Key type of the store entry
 * @param <TV> Type of the objects in the transaction view. This is the type visible from the outside.
 * @param <CV> Type of the objects as they are stored in the internal map of committed values. This type is not visible from the outside.
 * @author Jan Wiemer
 */
public class JacisStoreImpl<K, TV, CV> extends JacisContainer.JacisStoreTransactionAdapter implements JacisStore<K, TV>, JacisStoreAdminInterface<K, TV, CV> {

  /**
   * Reference to the JACIS container this store belongs to
   */
  private final JacisContainer container;
  /**
   * The store identifier uniquely identifying this store inside the container
   */
  private final StoreIdentifier storeIdentifier;
  /**
   * The object type specification for the objects stored in this store
   */
  private final JacisObjectTypeSpec<K, TV, CV> spec;
  /**
   * The map containing the committed values of the objects (the core store)
   */
  private final ConcurrentHashMap<K, StoreEntry<K, TV, CV>> store = new ConcurrentHashMap<>();
  /**
   * A Map assigning each active transaction handle the transactional view on this store
   */
  private final Map<JacisTransactionHandle, JacisStoreTxView<K, TV, CV>> txViewMap = new ConcurrentWeakHashMap<>();
  /**
   * Mutex / Lock to synchronize changes on the committed entries of the store (specially during internalCommit)
   */
  private final ReadWriteLock storeAccessLock;
  /**
   * The object adapter defining how to copy objects from the committed view to a transactional view and back
   */
  private final JacisObjectAdapter<TV, CV> objectAdapter;
  /**
   * The registry of (unique or not unique) indexes for this store
   */
  private final JacisIndexRegistry<K, TV> indexRegistry;
  /**
   * The registry of tracked views for this store that are kept up to date on each commit automatically
   */
  private final TrackedViewRegistry<K, TV> trackedViewRegistry;
  /**
   * List of listeners notified on each modification on the committed values in the store
   */
  private final List<JacisModificationListener<K, TV>> modificationListeners = new CopyOnWriteArrayList<>();

  public JacisStoreImpl(JacisContainer container, StoreIdentifier storeIdentifier, JacisObjectTypeSpec<K, TV, CV> spec) {
    this.container = container;
    this.storeIdentifier = storeIdentifier;
    this.spec = spec;
    this.objectAdapter = spec.getObjectAdapter();
    this.trackedViewRegistry = new TrackedViewRegistry<>(this, spec.isCheckViewsOnCommit());
    this.indexRegistry = new JacisIndexRegistry<>(this, spec.isCheckViewsOnCommit());
    this.storeAccessLock = spec.isSyncStoreOnContainerTransaction() ? container.getTransactionDemarcationLock() : new ReentrantReadWriteLock(true); // by default the store accesses are synced on the whole container TX
    registerModificationListener(trackedViewRegistry);
  }

  @Override
  public JacisStore<K, TV> getStore() {
    return this;
  }

  @Override
  public StoreIdentifier getStoreIdentifier() {
    return storeIdentifier;
  }

  @Override
  public JacisContainer getContainer() {
    return container;
  }

  @Override
  public JacisObjectTypeSpec<K, TV, CV> getObjectTypeSpec() {
    return spec;
  }

  @Override
  public List<JacisModificationListener<K, TV>> getModificationListeners() {
    return modificationListeners;
  }

  @Override
  public JacisStore<K, TV> registerModificationListener(JacisModificationListener<K, TV> listener) {
    if (!getObjectTypeSpec().isTrackOriginalValueEnabled()) {
      throw new IllegalStateException("Registering modification listeners is only supported if original values are tracked, but they are not tracked for " + this + "! Trying to register listener: " + listener);
    }
    modificationListeners.add(listener);
    return this;
  }

  @Override
  public JacisObjectAdapter<TV, CV> getObjectAdapter() {
    return objectAdapter;
  }

  @Override
  public TrackedViewRegistry<K, TV> getTrackedViewRegistry() {
    return trackedViewRegistry;
  }

  JacisIndexRegistry<K, TV> getIndexRegistry() {
    return indexRegistry;
  }

  public JacisIndexRegistryTxView<K, TV> getIndexRegistryTransactionView() {
    JacisStoreTxView<K, TV, CV> txView = getTxView(false);
    return txView == null ? null : txView.getIndexRegistryTxView();
  }

  @Override
  public <IK> JacisNonUniqueIndex<IK, K, TV> createNonUniqueIndex(String indexName, Function<TV, IK> indexKeyFunction) {
    return indexRegistry.createNonUniqueIndex(indexName, indexKeyFunction);
  }


  @Override
  public <IK> JacisNonUniqueIndex<IK, K, TV> getNonUniqueIndex(String indexName) {
    return indexRegistry.getNonUniqueIndex(indexName);
  }

  @Override
  public <IK> JacisNonUniqueMultiIndex<IK, K, TV> createNonUniqueMultiIndex(String indexName, Function<TV, Set<IK>> indexKeyFunction) {
    return indexRegistry.createNonUniqueMultiIndex(indexName, indexKeyFunction);
  }

  public <IK> JacisNonUniqueMultiIndex<IK, K, TV> getNonUniqueMultiIndex(String indexName) {
    return indexRegistry.getNonUniqueMultiIndex(indexName);
  }

  @Override
  public <IK> JacisUniqueIndex<IK, K, TV> createUniqueIndex(String indexName, Function<TV, IK> indexKeyFunction) {
    return indexRegistry.createUniqueIndex(indexName, indexKeyFunction);
  }

  @Override
  public <IK> JacisUniqueIndex<IK, K, TV> getUniqueIndex(String indexName) {
    return indexRegistry.getUniqueIndex(indexName);
  }

  @Override
  public List<String> getAllIndexDefinitions() {
    return indexRegistry.getAllIndexDefinitions();
  }

  @Override
  public JacisReadOnlyTransactionContext createReadOnlyTransactionView(String withTxName) {
    JacisStoreTxView<K, TV, CV> originalTxView = getTxView(true);
    return new JacisStoreTxView<>(withTxName, originalTxView, true);
  }

  @Override
  public JacisReadOnlyTransactionContext createReadOnlyTransactionViewUnsafe(String withTxName) {
    JacisStoreTxView<K, TV, CV> originalTxView = getTxView(true);
    return new JacisStoreTxView<>(withTxName, originalTxView, false);
  }

  @Override
  public void startReadOnlyTransactionWithContext(JacisReadOnlyTransactionContext readOnlyTxContext) {
    if (!(readOnlyTxContext instanceof JacisStoreTxView)) {
      throw new IllegalArgumentException("Passed illegal transactional context: " + readOnlyTxContext);
    }
    JacisStoreTxView<K, TV, CV> oldTxView = getTxView(false);
    if (oldTxView != null) {
      throw new IllegalStateException("Failed to start a read only transaction while another transaction is active! Active transaction: " + oldTxView.getTransaction() + ", passed transaction context: " + readOnlyTxContext + ", Thread: " + Thread.currentThread().getName());
    }
    @SuppressWarnings("unchecked")
    JacisStoreTxView<K, TV, CV> newTx = (JacisStoreTxView<K, TV, CV>) readOnlyTxContext;
    setTransactionContext(newTx);
  }

  public boolean isInReadOnlyTransaction() {
    JacisStoreTxView<K, TV, CV> txView = getTxView(false);
    return txView != null && txView.isReadOnly();
  }

  public boolean isCommitPending() {
    JacisStoreTxView<K, TV, CV> txView = getTxView(false);
    return txView != null && txView.isCommitPending();
  }

  public boolean hasObjectsUpdatedInCurrentTxView() {
    JacisStoreTxView<K, TV, CV> txView = getTxView(false);
    return txView != null && txView.getNumberOfUpdatedEntries() > 0;
  }

  @Override
  public boolean containsKey(K key) {
    JacisStoreTxView<K, TV, CV> txView = getTxView();
    StoreEntryTxView<K, TV, CV> entryTxView = txView == null ? null : txView.getEntryTxView(key);
    if (entryTxView != null) {
      return entryTxView.isNotNull();
    }
    StoreEntry<K, TV, CV> coreEntry = store.get(key);
    return coreEntry != null && coreEntry.isNotNull();
  }

  @Override
  public boolean isUpdated(K key) {
    JacisStoreTxView<K, TV, CV> txView = getTxView();
    StoreEntryTxView<K, TV, CV> entryTxView = txView == null ? null : txView.getEntryTxView(key);
    return entryTxView != null && entryTxView.isUpdated();
  }

  @Override
  public boolean isStale(K key) {
    JacisStoreTxView<K, TV, CV> txView = getTxView();
    StoreEntryTxView<K, TV, CV> entryTxView = txView == null ? null : txView.getEntryTxView(key);
    return entryTxView != null && entryTxView.isStale(txView);
  }

  @Override
  public void checkStale(K key) throws JacisStaleObjectException {
    withWriteLock(runnableWrapper(() -> { // preventing concurrent modification of core store
      JacisStoreTxView<K, TV, CV> txView = getTxView();
      StoreEntryTxView<K, TV, CV> entryTxView = txView == null ? null : txView.getEntryTxView(key);
      if (entryTxView != null) {
        entryTxView.assertNotStale(txView);
      }
    }));
  }

  @Override
  public TV get(K key) {
    return get(key, getOrCreateTxView());
  }

  @Override
  public TV getReadOnly(K key) {
    return getReadOnly(key, getTxView());
  }

  @Override
  public TV lockReadOnly(K key) {
    return lockReadOnly(key, getOrCreateTxView());
  }

  @Override
  public void unlock(K key) {
    JacisStoreTxView<K, TV, CV> txView = getTxView();
    if (txView != null) {
      txView.removeOptimisticLock(key);
    }
  }

  @Override
  public <P> P getProjectionReadOnly(K key, Function<TV, P> projection) {
    return projection.apply(getReadOnly(key));
  }

  /**
   * @return a stream of all keys currently stored in the store. Note that the keys added by any pending transactions are contained (with null values if not yet committed).
   */
  private Stream<K> keyStream() {
    return store.keySet().stream(); // store contains also new entries (with null value)! Therefore, iterating the keys is usually enough
  }

  @Override
  public Stream<TV> stream() { // Note this method will clone all objects into the TX view!
    JacisStoreTxView<K, TV, CV> txView = getOrCreateTxView();
    return keyStream().map(k -> get(k, txView)).filter(Objects::nonNull);
  }

  @Override
  public Stream<TV> streamReadOnly() {
    JacisStoreTxView<K, TV, CV> txView = getTxView();
    return keyStream().map(k -> getReadOnly(k, txView)).filter(Objects::nonNull);
  }

  public Stream<TV> streamAndLockReadOnly() {
    JacisStoreTxView<K, TV, CV> txView = getTxView();
    return keyStream().map(k -> lockReadOnly(k, txView)).filter(Objects::nonNull);
  }

  @Override
  public Stream<TV> stream(Predicate<TV> filter) {
    if (filter != null) {
      JacisStoreTxView<K, TV, CV> txView = getTxView();
      JacisStoreTxView<K, TV, CV> txViewNew = getOrCreateTxView();
      return keyStream().map(k -> pair(k, getReadOnly(k, txView))).filter(e -> e.getVal() != null && filter.test(e.getVal())).map(e -> get(e.getKey(), txViewNew));
    } else {
      return stream();
    }
  }

  @Override
  public Stream<TV> streamReadOnly(Predicate<TV> filter) {
    if (filter != null) {
      JacisStoreTxView<K, TV, CV> txView = getTxView();
      return keyStream().map(k -> getReadOnly(k, txView)).filter(v -> v != null && filter.test(v));
    } else {
      return streamReadOnly();
    }
  }

  public Stream<TV> streamAndLockReadOnly(Predicate<TV> filter) {
    if (filter != null) {
      JacisStoreTxView<K, TV, CV> txView = getTxView();
      return keyStream().map(k -> lockReadOnly(k, txView)).filter(v -> v != null && filter.test(v));
    } else {
      return streamAndLockReadOnly();
    }
  }

  public Stream<KeyValuePair<K, TV>> streamKeyValuePairsReadOnly(Predicate<TV> filter) {
    JacisStoreTxView<K, TV, CV> txView = getTxView();
    if (filter != null) {
      return keyStream().map(k -> new KeyValuePair<>(k, getReadOnly(k, txView))).filter(p -> p.getVal() != null && filter.test(p.getVal()));
    } else {
      return keyStream().map(k -> new KeyValuePair<>(k, getReadOnly(k, txView))).filter(p -> p.getVal() != null);
    }
  }

  @Override
  public Stream<KeyValuePair<K, TV>> streamAllUpdated(Predicate<TV> filter) {
    JacisStoreTxView<K, TV, CV> txView = getTxView();
    return txView.getAllEntryTxViews().stream() //
        .filter(entryTxView -> entryTxView.isUpdated() && (filter == null || filter.test(entryTxView.getValue()))) //
        .map(entryTxView -> new KeyValuePair<>(entryTxView.getKey(), entryTxView.getValue()));
  }

  @Override
  public List<TV> getAll() {
    return getAll(null);
  }

  @Override
  public List<TV> getAll(Predicate<TV> filter) {
    return stream(filter).collect(Collectors.toList());
  }

  @Override
  public List<TV> getAllReadOnly() {
    return getAllReadOnly(null);
  }

  @Override
  public List<TV> getAllReadOnly(Predicate<TV> filter) {
    return streamReadOnly(filter).collect(Collectors.toList());
  }

  @Override
  public List<TV> lockAllReadOnly(Predicate<TV> filter) {
    return streamAndLockReadOnly(filter).collect(Collectors.toList());
  }

  @Override
  public List<TV> getReadOnlySnapshot(Predicate<TV> filter) {
    ArrayList<TV> res = new ArrayList<>(size());
    if (filter == null) {
      return getReadOnlySnapshot();
    }
    for (StoreEntry<K, TV, CV> storeEntry : store.values()) {
      CV cv = storeEntry.getValue();
      if (cv != null) {
        TV ev = getObjectAdapter().cloneCommitted2ReadOnlyTxView(cv);
        if (filter.test(ev)) {
          res.add(ev);
        }
      }
    }
    return res;
  }

  @Override
  public List<TV> getReadOnlySnapshot() {
    ArrayList<TV> res = new ArrayList<>(size());
    for (StoreEntry<K, TV, CV> storeEntry : store.values()) {
      CV cv = storeEntry.getValue();
      if (cv != null) {
        TV ev = getObjectAdapter().cloneCommitted2ReadOnlyTxView(cv);
        res.add(ev);
      }
    }
    return res;
  }

  @Override
  public List<TV> getAllAtomic(Predicate<TV> filter) {
    return computeAtomic(() -> getAll(filter));
  }

  @Override
  public List<TV> getAllReadOnlyAtomic(Predicate<TV> filter) {
    return computeAtomic(() -> getAllReadOnly(filter));
  }

  @Override
  public List<TV> lockAllReadOnlyAtomic(Predicate<TV> filter) {
    return computeAtomic(() -> lockAllReadOnly(filter));
  }

  @Override
  public List<TV> getReadOnlySnapshotAtomic() {
    return computeAtomic(this::getReadOnlySnapshot);
  }

  @Override
  public List<TV> getReadOnlySnapshotAtomic(Predicate<TV> filter) {
    return computeAtomic(() -> getReadOnlySnapshot(filter));
  }

  @Override
  public List<TV> getPageReadOnly(Predicate<TV> filter, Comparator<TV> comparator, long offset, long pageSize) {
    return streamReadOnly(filter).sorted(comparator).skip(offset).limit(pageSize).collect(Collectors.toList());
  }

  @Override
  public <PV> List<PV> getWrapperPageReadOnly(Function<TV, PV> wrapper, Predicate<PV> filter, Comparator<PV> comparator, long offset, long pageSize) {
    return streamReadOnly().map(wrapper).filter(filter).sorted(comparator).skip(offset).limit(pageSize).collect(Collectors.toList());
  }

  @Override
  public void update(K key, TV value) throws JacisTransactionAlreadyPreparedForCommitException {
    JacisStoreTxView<K, TV, CV> txView = getOrCreateTxView().assertWritable();
    if (txView.isCommitPending()) {
      throw new JacisTransactionAlreadyPreparedForCommitException("Failed to update " + key + " because transaction is already prepared for commit: " + txView);
    }
    StoreEntryTxView<K, TV, CV> entryTxView = getOrCreateEntryTxView(txView, key);
    txView.updateValue(entryTxView, value);
  }

  @Override
  public void update(Collection<TV> values, Function<TV, K> keyExtractor) throws JacisTransactionAlreadyPreparedForCommitException {
    JacisStoreTxView<K, TV, CV> txView = getOrCreateTxView().assertWritable();
    if (txView.isCommitPending()) {
      throw new JacisTransactionAlreadyPreparedForCommitException("Failed to execute bulk update for " + values.size() + " objects because transaction is already prepared for commit: " + txView);
    } else if (values == null || values.isEmpty()) {
      return;
    } else if (keyExtractor == null) {
      throw new NullPointerException("keyExtractor parameter must not be null!");
    }
    for (TV value : values) {
      K key = keyExtractor.apply(value);
      StoreEntryTxView<K, TV, CV> entryTxView = getOrCreateEntryTxView(txView, key);
      txView.updateValue(entryTxView, value);
    }
  }

  @Override
  public void remove(K key) throws JacisTransactionAlreadyPreparedForCommitException {
    update(key, null);
  }

  @Override
  public TV refresh(K key) { // refresh with committed version -> discard all changes made by the current TX
    JacisStoreTxView<K, TV, CV> txView = getTxView();
    if (txView != null) {
      txView.refreshTxViewEntryFromCommitted(key, true);
    }
    return get(key);
  }

  @Override
  public TV refreshIfNotUpdated(K key) { // if not updated: refresh with committed version -> discard all changes made by the current TX
    JacisStoreTxView<K, TV, CV> txView = getTxView();
    if (txView != null) {
      txView.refreshTxViewEntryFromCommitted(key, false);
    }
    return get(key);
  }

  @Override
  public <ST> void initStoreNonTransactional(List<ST> entries, Function<ST, K> keyExtractor, Function<ST, TV> valueExtractor, int nThreads) {
    withWriteLock(() -> {
      if (!store.isEmpty()) {
        throw new IllegalStateException("Store must be empty before initialization!");
      }
      List<JacisModificationListener<K, TV>> modListeners = new ArrayList<>(getModificationListeners());
      if (nThreads <= 1 || entries.size() <= 1000) {
        for (ST entry : entries) {
          K key = keyExtractor.apply(entry);
          TV val = valueExtractor.apply(entry);
          store.put(key, new StoreEntry<>(this, key, val));
          for (JacisModificationListener<K, TV> listener : modListeners) {
            listener.onModification(key, null, val, null);
          }
        }
      } else {
        boolean allModificationListenersThreadSafe = modListeners.stream().allMatch(JacisModificationListener::isThreadSafe);
        int usedThreads = Math.min(nThreads, entries.size() / 100);
        List<Thread> threads = new ArrayList<>(usedThreads);
        int perThread = (entries.size() - 1) / usedThreads + 1;
        for (int threadNr = 0; threadNr < usedThreads; threadNr++) {
          int from = threadNr * perThread;
          int to = Math.min(entries.size(), (threadNr + 1) * perThread);
          threads.add(new Thread("initStoreThread" + threadNr + "[" + from + "-" + to + " for " + JacisStoreImpl.this + "]") {
            @Override
            public void run() {
              if (allModificationListenersThreadSafe) { // THREAD-SAFE
                for (int idx = from; idx < to; idx++) {
                  ST entry = entries.get(idx);
                  K key = keyExtractor.apply(entry);
                  TV val = valueExtractor.apply(entry);
                  store.put(key, new StoreEntry<>(JacisStoreImpl.this, key, val));
                  for (JacisModificationListener<K, TV> listener : modListeners) {
                    listener.onModification(key, null, val, null); // for performance reasons we skip synchronization if listener is thread safe
                  }
                } // end of for loop
              } else { // NOT THREAD-SAFE
                for (int idx = from; idx < to; idx++) {
                  ST entry = entries.get(idx);
                  K key = keyExtractor.apply(entry);
                  TV val = valueExtractor.apply(entry);
                  store.put(key, new StoreEntry<>(JacisStoreImpl.this, key, val));
                  for (JacisModificationListener<K, TV> listener : modListeners) {
                    if (listener.isThreadSafe()) {
                      listener.onModification(key, null, val, null); // for performance reasons we skip synchronization if listener is thread safe
                    } else {
                      //noinspection SynchronizationOnLocalVariableOrMethodParameter
                      synchronized (listener) { // if listener is *not* thread safe we need to synchronize access on the listener (note: we are initializing the store with multiple threads)
                        listener.onModification(key, null, val, null);
                      }
                    }
                  }
                } // end of for loop
              } // end of THREAD-SAFE if ... else ... block
            } // end of run() method
          });
        }
        threads.forEach(Thread::start);
        try {
          for (Thread thread : threads) {
            thread.join();
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      return null;
    });
  }

  @Override
  public void initStoreNonTransactional(List<TV> values, Function<TV, K> keyExtractor, int nThreads) {
    initStoreNonTransactional(values, keyExtractor, v -> v, nThreads);
  }

  @Override
  public void initStoreNonTransactional(List<KeyValuePair<K, TV>> entries, int nThreads) {
    initStoreNonTransactional(entries, KeyValuePair::getKey, KeyValuePair::getVal, nThreads);
  }

  @Override
  public int size() { // heuristic (due to concurrent access)
    return store.size();
  }

  public int getNumberOfActivTxViews() {
    return txViewMap.size();
  }

  @Override
  public void executeAtomic(Runnable atomicOperation) { // Execute an atomic operation for the current store. No internalCommit of any other TX and no other atomic action for this store will interleave.
    withReadLock(runnableWrapper(atomicOperation));
  }

  @Override
  public <R> R computeAtomic(Supplier<R> atomicOperation) { // Execute an atomic operation for the current store. No internalCommit of any other TX and no other atomic action for this store will interleave.
    return withReadLock(atomicOperation);
  }

  @Override
  public void executeGlobalAtomic(Runnable atomicOperation) { // Execute a global atomic operation. No prepare / commit / rollback of any other TX and no other global atomic action for any store will interleave.
    executeAtomic(() -> container.executeGlobalAtomic(atomicOperation));
  }

  @Override
  public <R> R computeGlobalAtomic(Supplier<R> atomicOperation) { // Execute a global atomic operation for the current store. No prepare / commit / rollback of any other TX and no other global atomic action for any store will interleave.
    return computeAtomic(() -> container.computeGlobalAtomic(atomicOperation));
  }

  @Override
  public <C> C accumulate(C target, BiConsumer<C, TV> accumulator) {
    for (TV entryTxView : getAllReadOnly(null)) {
      accumulator.accept(target, entryTxView);
    }
    return target;
  }

  @Override
  public <C> C accumulateAtomic(C target, BiConsumer<C, TV> accumulator) {
    return computeAtomic(() -> accumulate(target, accumulator));
  }

  @Override
  public TV getTransactionStartValue(K key) {
    assertTrackOriginalValue();
    JacisStoreTxView<K, TV, CV> txView = getTxView();
    StoreEntryTxView<K, TV, CV> entryTxView = txView == null ? null : txView.getEntryTxView(key);
    return entryTxView == null ? null : entryTxView.getOrigValue(); // if TX never touched the object we return null
  }

  @Override
  public TV getCommittedValue(K key) {
    StoreEntry<K, TV, CV> committedEntry = getCommittedEntry(key);
    return committedEntry == null ? null : objectAdapter.cloneCommitted2ReadOnlyTxView(committedEntry.getValue());
  }

  @Override
  public boolean hasTransactionView(K key) {
    JacisStoreTxView<K, TV, CV> txView = getTxView();
    return txView != null && txView.containsTxView(key);
  }

  @Override
  public long getTransactionViewVersion(K key) {
    JacisStoreTxView<K, TV, CV> txView = getTxView();
    StoreEntryTxView<K, TV, CV> entryTxView = txView == null ? null : txView.getEntryTxView(key);
    if (entryTxView != null) {
      return entryTxView.getOrigVersion();
    }
    StoreEntry<K, TV, CV> committedEntry = getCommittedEntry(key);
    return committedEntry != null ? committedEntry.getVersion() : -1;
  }

  @Override
  public Long getLockedVersion(K key) {
    JacisStoreTxView<K, TV, CV> txView = getTxView();
    return txView == null ? null : txView.getOptimisticLockVersion(key);
  }

  @Override
  public long getCommittedVersion(K key) {
    StoreEntry<K, TV, CV> committedEntry = getCommittedEntry(key);
    return committedEntry != null ? committedEntry.getVersion() : -1;
  }

  @Override
  public StoreEntryInfo<K, TV> getObjectInfo(K key) {
    JacisStoreTxView<K, TV, CV> txView = getTxView();
    StoreEntry<K, TV, CV> committedEntry = getCommittedEntry(key);
    StoreEntryTxView<K, TV, CV> entryTxView = txView == null ? null : txView.getEntryTxView(key);
    return new StoreEntryInfo<>(key, committedEntry, entryTxView, txView);
  }

  @Override
  public List<JacisTransactionInfo.StoreTxInfo> getTransactionInfos() {
    return safeGetAllTxViews().stream().map(txView -> new JacisTransactionInfo.StoreTxInfo(this, txView)).collect(Collectors.toList());
  }

  @Override
  public synchronized void clear() {
    storeAccessLock.writeLock().lock();// <======= **WRITE** LOCK =====
    try {
      Collection<JacisStoreTxView<K, TV, CV>> txs = safeGetAllTxViews();
      for (JacisStoreTxView<K, TV, CV> txCtx : txs) {
        txCtx.invalidate("store cleared");
      }
      store.clear();
      trackedViewRegistry.clearViews();
      indexRegistry.clearIndices();
    } finally {
      storeAccessLock.writeLock().unlock();// <======= **WRITE** UNLOCK =====
    }
  }

  // ======================================================================================
  // transaction demarcation methods
  // ======================================================================================

  @Override
  protected void internalPrepare(JacisTransactionHandle transaction) {
    new StoreTxDemarcationExecutor().executePrepare(this, transaction, storeAccessLock);
  }

  @Override
  protected void internalCommit(JacisTransactionHandle transaction) {
    new StoreTxDemarcationExecutor().executeCommit(this, transaction, storeAccessLock);
  }

  @Override
  protected void internalRollback(JacisTransactionHandle transaction) {
    new StoreTxDemarcationExecutor().executeRollback(this, transaction, storeAccessLock);
  }

  @Override
  protected void internalDestroy(JacisTransactionHandle transaction) {
    new StoreTxDemarcationExecutor().executeDestroy(this, transaction, storeAccessLock);
  }

  // ======================================================================================
  // helper methods to access entries
  // ======================================================================================

  private TV getReadOnly(K key, JacisStoreTxView<K, TV, CV> txView) {
    StoreEntryTxView<K, TV, CV> entryTxView = txView == null ? null : txView.getEntryTxView(key);
    if (entryTxView != null) {
      return entryTxView.getValue();
    } else {
      StoreEntry<K, TV, CV> committedEntry = getCommittedEntry(key);
      return committedEntry == null ? null : objectAdapter.cloneCommitted2ReadOnlyTxView(committedEntry.getValue());
    }
  }

  private TV lockReadOnly(K key, JacisStoreTxView<K, TV, CV> txView) {
    assert txView != null;
    StoreEntryTxView<K, TV, CV> entryTxView = txView.getEntryTxView(key);
    if (entryTxView != null) {
      txView.addOptimisticLock(key, entryTxView);
      return entryTxView.getValue();
    } else {
      StoreEntry<K, TV, CV> committedEntry = getCommittedEntry(key);
      txView.addOptimisticLock(key, committedEntry);
      return committedEntry == null ? null : objectAdapter.cloneCommitted2ReadOnlyTxView(committedEntry.getValue());
    }
  }

  public TV get(K key, JacisStoreTxView<K, TV, CV> txView) {
    if (txView.isCommitPending() && getObjectTypeSpec().isSwitchToReadOnlyModeInPrepare()) {
      return getReadOnly(key);
    }
    return getOrCreateEntryTxView(txView, key).getValue();
  }

  // ======================================================================================
  // helper methods to deal with transaction views of entries
  // ======================================================================================

  private final static class StoreEntryTxViewReference<K, TV, CV> {
    StoreEntryTxView<K, TV, CV> ref;
  }

  private StoreEntryTxView<K, TV, CV> getOrCreateEntryTxView(JacisStoreTxView<K, TV, CV> txView, K key) {
    StoreEntryTxView<K, TV, CV> entryTxView = txView.getEntryTxView(key);
    if (entryTxView == null) {
      StoreEntryTxViewReference<K, TV, CV> newTxView = new StoreEntryTxViewReference<>();
      updateCommittedEntry(key, (k, existingCommittedEntry) -> {
        StoreEntry<K, TV, CV> newCommittedEntry = existingCommittedEntry != null ? existingCommittedEntry : new StoreEntry<>(this, key);
        newTxView.ref = txView.createTxViewEntry(newCommittedEntry);
        return newCommittedEntry;
      });
      entryTxView = newTxView.ref;
    }
    return entryTxView;
  }

  // ======================================================================================
  // helper methods to deal with committed entries
  // ======================================================================================

  StoreEntry<K, TV, CV> getCommittedEntry(K key) {
    return store.get(key);
  }

  void updateCommittedEntry(K key, BiFunction<K, StoreEntry<K, TV, CV>, StoreEntry<K, TV, CV>> updateFunction) {
    store.compute(key, updateFunction);
  }

  void checkRemoveCommittedEntries(JacisStoreTxView<K, TV, CV> finishedTxView) {
    // collect and delete committed entries referring a null value and not referenced by any TX (garbage)
    for (StoreEntryTxView<K, TV, CV> entryTxView : finishedTxView.getAllEntryTxViews()) {
      K key = entryTxView.getKey();
      updateCommittedEntry(key, (k, entryCommitted) -> {
        if (entryCommitted != null && entryCommitted.unreferencedByTxViewAnCheckIfGarbage()) {
          return null;
        }
        return entryCommitted;
      });
    }
  }

  // ======================================================================================
  // synchronized execution
  // ======================================================================================

  @SuppressWarnings("UnusedReturnValue")
  private <R> R withWriteLock(Supplier<R> task) {
    storeAccessLock.writeLock().lock(); // <======= **WRITE** LOCK =====
    try {
      return task.get();
    } finally {
      storeAccessLock.writeLock().unlock(); // <======= **WRITE** UNLOCK =====
    }
  }

  private <R> R withReadLock(Supplier<R> task) {
    storeAccessLock.readLock().lock(); // <======= **READ** LOCK =====
    try {
      return task.get();
    } finally {
      storeAccessLock.readLock().unlock(); // <======= **READ** UNLOCK =====
    }
  }

  private Supplier<Object> runnableWrapper(Runnable r) {
    return () -> {
      r.run();
      return null;
    };
  }

  // ======================================================================================
  // private methods to maintain the TX view
  // ======================================================================================

  JacisStoreTxView<K, TV, CV> getTxView() {
    return getTxView(false);
  }

  private JacisStoreTxView<K, TV, CV> getOrCreateTxView() {
    return getTxView(true);
  }

  private JacisStoreTxView<K, TV, CV> getTxView(boolean createIfAbsent) {
    JacisTransactionHandle transaction = container.getCurrentTransaction(createIfAbsent);
    return getTxView(transaction, createIfAbsent);
  }

  JacisStoreTxView<K, TV, CV> getTxView(JacisTransactionHandle transaction, boolean createIfAbsent) {
    if (transaction == null) {
      if (createIfAbsent) {
        throw new IllegalArgumentException("Can not create transaction view without transaction!");
      }
      return null;
    }
    JacisStoreTxView<K, TV, CV> txView = txViewMap.get(transaction);
    if (txView == null && createIfAbsent) {
      txView = new JacisStoreTxView<>(this, transaction);
      txViewMap.put(transaction, txView);
    }
    return txView;
  }

  private void setTransactionContext(JacisStoreTxView<K, TV, CV> newTxContext) {
    if (newTxContext == null) {
      throw new IllegalArgumentException("setting >null< as transaction context is not allowed!");
    }
    JacisTransactionHandle transaction = container.getCurrentTransaction(true);
    txViewMap.put(transaction, newTxContext);
  }

  void notifyTxViewDestroyed(JacisStoreTxView<K, TV, CV> txView) {
    txViewMap.remove(txView.getTransaction());
  }

  private Collection<JacisStoreTxView<K, TV, CV>> safeGetAllTxViews() {
    Collection<JacisStoreTxView<K, TV, CV>> txs;
    txs = new ArrayList<>(txViewMap.size() + 5); // safety, size may vary
    try {
      txs.addAll(txViewMap.values());
    } catch (NoSuchElementException e) {
      // all entries still there have been visited
    }
    return txs;
  }

  // ======================================================================================
  // other private helper methods
  // ======================================================================================

  private void assertTrackOriginalValue() {
    if (!spec.isTrackOriginalValueEnabled()) {
      throw new UnsupportedOperationException("Track original values not supported by " + spec + "!");
    }
  }

  private KeyValuePair<K, TV> pair(K key, TV val) {
    return new KeyValuePair<>(key, val);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "-(" + spec + ": #" + store.size() + " entries)";
  }

}
