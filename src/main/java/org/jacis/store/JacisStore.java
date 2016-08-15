/*
 * Copyright (c) 2016. Jan Wiemer
 */
package org.jacis.store;

import org.jacis.container.JacisContainer;
import org.jacis.container.JacisContainer.StoreIdentifier;
import org.jacis.container.JacisObjectTypeSpec;
import org.jacis.container.JacisTransactionHandle;
import org.jacis.exception.JacisStaleObjectException;
import org.jacis.exception.JacisTransactionAlreadyPreparedForCommitException;
import org.jacis.plugin.JacisModificationListener;
import org.jacis.plugin.objectadapter.JacisObjectAdapter;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Storing a single type of objects.
 *
 * All operations checking or returning entries of the store operate on the committed values merged with the
 * current transactional view (obtained with the currently active transaction handle from the map {@link #txViewMap}).
 * This means that first the transactional view is checked if it contains an entry for the desired key.
 * If so this entry is returned, otherwise the committed value from the core store (see {@link #store}) is returned.
 * Note that if an object is deleted in a transaction an entry with the value 'null' remains in the transactional view.
 * Therefore also deletions are properly handled with respect to isolation.
 *
 * @param <K> Key type of the store entry
 * @param <TV> Type of the objects in the transaction view. This is the type visible from the outside.
 * @param <CV> Type of the objects as they are stored in the internal map of committed values. This type is not visible from the outside.
 * @author Jan Wiemer
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class JacisStore<K, TV, CV> extends JacisContainer.JacisStoreTransactionAdapter {

  /** Reference to the JACIS container this store belongs to */
  private final JacisContainer container;
  /** The store identifier uniquely identifying this store inside the container */
  private final StoreIdentifier storeIdentifier;
  /** The object type specification for the objects stored in this store*/
  private final JacisObjectTypeSpec<K, TV, CV> spec;
  /** The map containing the committed values of the objects (the core store) */
  private final ConcurrentHashMap<K, StoreEntry<K, TV, CV>> store = new ConcurrentHashMap<>();
  /** A Map assigning each active transaction handle the transactional view on this store */
  private final Map<JacisTransactionHandle, JacisStoreTxView<K, TV, CV>> txViewMap = Collections.synchronizedMap(new WeakHashMap<JacisTransactionHandle, JacisStoreTxView<K, TV, CV>>());
  /** Mutex / Lock to synchronize changes on the committed entries of the store (specially during internalCommit) */
  private final ReadWriteLock storeAccessLock = new ReentrantReadWriteLock(true);
  /** The object adapter defining how to copy objects from the committed view to a transactional view and back */
  private final JacisObjectAdapter<TV, CV> objectAdapter;
  /** The registry of tracked views for this store that are kept up to date on each commit automatically */
  private final TrackedViewRegistry<K, TV, CV> trackedViewRegistry;
  /** List of listeners notified on each modification on the committed values in the store */
  private final List<JacisModificationListener<K, TV>> modificationListeners = new ArrayList<>();

  public JacisStore(JacisContainer container, StoreIdentifier storeIdentifier, JacisObjectTypeSpec<K, TV, CV> spec) {
    this.container = container;
    this.storeIdentifier = storeIdentifier;
    this.spec = spec;
    this.objectAdapter = spec.getObjectAdapter();
    this.trackedViewRegistry = new TrackedViewRegistry<>(this, spec.isCheckViewsOnCommit());
    registerModificationListener(trackedViewRegistry);
  }

  /** @return the store identifier uniquely identifying this store inside the container */
  public StoreIdentifier getStoreIdentifier() {
    return storeIdentifier;
  }

  /** @return the reference to the JACIS container this store belongs to */
  public JacisContainer getContainer() {
    return container;
  }

  /** @return the object type specification for the objects stored in this store */
  public JacisObjectTypeSpec<K, TV, CV> getObjectTypeSpec() {
    return spec;
  }

  /** @return the list of listeners notified on each modification on the committed values in the store */
  public List<JacisModificationListener<K, TV>> getModificationListeners() {
    return modificationListeners;
  }

  /**
   * Add the passed listener (implementing the interface {@link JacisModificationListener}).
   * The listener will be notified on each modification on the committed values in the store.
   *
   * @param listener the listener to notify
   * @return this store for method chaining
   */
  public JacisStore<K, TV, CV> registerModificationListener(JacisModificationListener<K, TV> listener) {
    if (!getObjectTypeSpec().isTrackOriginalValueEnabled()) {
      throw new IllegalStateException("Registering modification listeners is only supported if original values are tracked, but they are not tracked for " + this + "! Trying to register listener: " + listener);
    }
    modificationListeners.add(listener);
    return this;
  }

  /** @return the object adapter defining how to copy objects from the committed view to a transactional view and back */
  public JacisObjectAdapter<TV, CV> getObjectAdapter() {
    return objectAdapter;
  }

  /** @return tte registry of tracked views for this store that are kept up to date on each commit automatically */
  public TrackedViewRegistry<K, TV, CV> getTrackedViewRegistry() {
    return trackedViewRegistry;
  }

  /**
   * Returns if the store contains an entry for the passed key.
   * Note that the method operates on the committed values merged with the current transactional view (see class description).
   *
   * @param key The key of the entry to check.
   * @return if the store contains an entry for the passed key.
   */
  public boolean containsKey(K key) {
    JacisStoreTxView<K, TV, CV> txView = getTxView();
    StoreEntryTxView<K, TV, CV> entryTxView = txView == null ? null : txView.getEntryTxView(key);
    if (entryTxView != null) {
      return entryTxView.isNotNull();
    }
    StoreEntry<K, TV, CV> coreEntry = store.get(key);
    return coreEntry != null && coreEntry.isNotNull();
  }

  /**
   * Returns if the object for the passed key has been updated in the current transaction.
   * Note that an update has to be explicitly called for an object (by calling {@link #update(Object, Object)}).
   * The check returns true if there exists a transactional view
   * and the updated flag of this entry (see {@link StoreEntryTxView#updated}) is set (set by the 'update' method).
   * Note that this method does not cause the referred object to be copied to thr transactional view.
   *
   * @param key The key of the entry to check.
   * @return if the object for the passed key has been updated in the current transaction.
   */
  public boolean isUpdated(K key) {
    JacisStoreTxView<K, TV, CV> txView = getTxView();
    StoreEntryTxView<K, TV, CV> entryTxView = txView == null ? null : txView.getEntryTxView(key);
    return entryTxView != null && entryTxView.isUpdated();
  }

  /**
   * Returns if the object for the passed key is stale.
   * An object is considered to be stale if after first reading it in the current transaction,
   * an updated version of the same object has been committed by another transaction.
   * Note that this method does not cause the referred object to be copied to thr transactional view.
   * @param key The key of the entry to check.
   * @return if the object for the passed key is stale.
   */
  public boolean isStale(K key) {
    JacisStoreTxView<K, TV, CV> txView = getTxView();
    StoreEntryTxView<K, TV, CV> entryTxView = txView == null ? null : txView.getEntryTxView(key);
    return entryTxView != null && entryTxView.isStale(txView);
  }

  /**
   * Checks if the object for the passed key is stale and throws a {@link JacisStaleObjectException} if so.
   * An object is considered to be stale if after first reading it in the current transaction,
   * an updated version of the same object has been committed by another transaction.
   * Note that this method does not cause the referred object to be copied to thr transactional view.
   *
   * @param key The key of the entry to check.
   * @throws JacisStaleObjectException thrown if the object for the passed key is stale.
   */
  public void checkStale(K key) throws JacisStaleObjectException {
    JacisStoreTxView<K, TV, CV> txView = getTxView();
    StoreEntryTxView<K, TV, CV> entryTxView = txView == null ? null : txView.getEntryTxView(key);
    if (entryTxView != null) {
      entryTxView.assertNotStale(txView);
    }
  }

  /**
   * Returns the value for the passed key.
   * Note that the method operates on the committed values merged with the current transactional view (see class description).
   * If the transactional view did not already contain the entry for the key it is copied to the transactional view now.
   *
   * @param key The key of the desired entry.
   * @return the value for the passed key.
   */
  public TV get(K key) {
    return getOrCreateEntryTxView(getOrCreateTxView(), key).getValue();
  }

  /**
   * Returns the value for the passed key.
   * If the object is already stored in the transactional view of the current transaction this value is returned.
   * Otherwise the behaviour depends on the object type:
   * If the object adapter for the store supports a read only mode, then a read only view on the committed value is returned.
   * Otherwise the committed entry for the key it is copied to the transactional view now.
   *
   * @param key The key of the desired entry.
   * @return the value for the passed key.
   */
  public TV getReadOnly(K key) {
    JacisStoreTxView<K, TV, CV> txView = getTxView();
    StoreEntryTxView<K, TV, CV> entryTxView = txView == null ? null : txView.getEntryTxView(key);
    if (entryTxView != null) {
      return entryTxView.getValue();
    } else {
      StoreEntry<K, TV, CV> committedEntry = getCommittedEntry(key);
      return committedEntry == null ? null : objectAdapter.cloneCommitted2ReadOnlyTxView(committedEntry.getValue());
    }
  }

  /**
   * Returns a read only projection of the object for the passed value.
   * First a read only view (if supported) of the object is obtained by the {@link #getReadOnly(Object)} method.
   * The projected is computed from the object by applying the passed projection function.
   *
   * @param key The key of the desired entry.
   * @param projection The projection function computing the desired return value (of the passed type 'P') from the object.
   * @param <P> The result type of the projection
   * @return a read only projection of the object for the passed value.
   */
  public <P> P getProjectionReadOnly(K key, Function<TV, P> projection) {
    return projection.apply(getReadOnly(key));
  }

  /** @return a stream of all keys currently stored in the store. Note that the keys added by any pending transactions are contained (with null values if not jet committed).  */
  private Stream<K> keyStream() {
    return store.keySet().stream(); // store contains also new entries (with null value)! Therefore iterating the keys is usually enough
  }

  /**
   * Returns a stream of all objects (noz 'null') currently stored in the store.
   * Note that the method operates on the committed values merged with the current transactional view (see class description).
   * If the transactional view did not already contain an entry it is copied to the transactional view now.
   *
   * @return a stream of all objects (noz 'null') currently stored in the store.
   */
  public Stream<TV> stream() { // Note this method will clone all objects into the TX view!
    return keyStream().map(this::get).filter(v -> v != null);
  }

  /**
   * Returns a stream of read only views for all objects (noz 'null') currently stored in the store.
   * Note that the method operates on the committed values merged with the current transactional view (see class description).
   * Further note that the behavior of the method is equivalent to the behavior of the {@link #getReadOnly} method for a single object.
   *
   * @return a stream of all objects (noz 'null') currently stored in the store.
   */
  public Stream<TV> streamReadOnly() {
    return keyStream().map(this::getReadOnly).filter(v -> v != null);
  }

  /**
   * Returns a stream of all objects (noz 'null') currently stored in the store filtered by the passed filter.
   * Note that the method operates on the committed values merged with the current transactional view (see class description).
   * If supported the filter predicate is checked on a read only view of the object (without cloning it).
   * Only the objects passing the filter are is copied to the transactional view (if they are not yet contained there).
   *
   * @param filter a filter predicate deciding if an object should be contained in the resulting stream ('null' means all objects should be contained)
   * @return a stream of all objects (noz 'null') currently stored in the store filtered by the passed filter.
   */
  public Stream<TV> stream(Predicate<TV> filter) {
    if (filter != null) {
      return keyStream().map(k -> pair(k, getReadOnly(k))).filter(e -> e.val != null && filter.test(e.val)).map(e -> get(e.key));
    } else {
      return stream();
    }
  }

  /**
   * Returns a stream of read only views for all objects (noz 'null') currently stored in the store filtered by the passed filter.
   * Note that the method operates on the committed values merged with the current transactional view (see class description).
   * If supported the filter predicate is checked on a read only view of the object (without cloning it).
   * Further note that the behavior of the method is equivalent to the behavior of the {@link #getReadOnly} method for a single object
   * (only the objects passing the filter may be copied to the transactional view if no read only view is supported (and they are not yet contained there)).
   *
   * @param filter a filter predicate deciding if an object should be contained in the resulting stream ('null' means all objects should be contained)
   * @return a stream of all objects (noz 'null') currently stored in the store filtered by the passed filter.
   */
  public Stream<TV> streamReadOnly(Predicate<TV> filter) {
    if (filter != null) {
      return keyStream().map(this::getReadOnly).filter(v -> v != null && filter.test(v));
    } else {
      return streamReadOnly();
    }
  }

  /**
   * Returns a list of all objects (noz 'null') currently stored in the store filtered by the passed filter.
   * The method uses the {@link #stream(Predicate)} method and collects the results to a list.
   *
   * @param filter a filter predicate deciding if an object should be contained in the resulting stream ('null' means all objects should be contained)
   * @return a list of all objects (noz 'null') currently stored in the store filtered by the passed filter.
   */
  public List<TV> getAll(Predicate<TV> filter) {
    return stream(filter).collect(Collectors.toList());
  }

  /**
   * Returns a list of read-only views for all objects (noz 'null') currently stored in the store filtered by the passed filter.
   * The method uses the {@link #streamReadOnly(Predicate)} method and collects the results to a list.
   *
   * @param filter a filter predicate deciding if an object should be contained in the resulting stream ('null' means all objects should be contained)
   * @return a list of read-only views for all objects (noz 'null') currently stored in the store filtered by the passed filter.
   */
  public List<TV> getAllReadOnly(Predicate<TV> filter) {
    return streamReadOnly(filter).collect(Collectors.toList());
  }

  /**
   * Returns a list of all objects (noz 'null') currently stored in the store filtered by the passed filter.
   * The method executes the {@link #getAll(Predicate)} method as an atomic operations.
   * Therefore this method is passed as functional parameter to the {@link #computeAtomic(Supplier)} method.
   * The execution of atomic operations can not overlap with the execution of other atomic operations (but normal operations may overlap).
   *
   * @param filter a filter predicate deciding if an object should be contained in the resulting stream ('null' means all objects should be contained)
   * @return a list of all objects (noz 'null') currently stored in the store filtered by the passed filter.
   */
  public List<TV> getAllAtomic(Predicate<TV> filter) {
    return computeAtomic(() -> getAll(filter));
  }

  /**
   * Returns a list of read-only views for all objects (noz 'null') currently stored in the store filtered by the passed filter.
   * The method executes the {@link #getAllReadOnly(Predicate)} method as an atomic operations.
   * Therefore this method is passed as functional parameter to the {@link #computeAtomic(Supplier)} method.
   * The execution of atomic operations can not overlap with the execution of other atomic operations (but normal operations may overlap).
   *
   * @param filter a filter predicate deciding if an object should be contained in the resulting stream ('null' means all objects should be contained)
   * @return a list of read-only views for all objects (noz 'null') currently stored in the store filtered by the passed filter.
   */
  public List<TV> getAllReadOnlyAtomic(Predicate<TV> filter) {
    return computeAtomic(() -> getAllReadOnly(filter));
  }

  /**
   * Update the object for the passed key with the passed object value.
   * Note that the passed object instance may be the same (modified) instance obtained from the store before,
   * but also can be another instance.
   * Internally the value of the transactional view (see {@link StoreEntryTxView#txValue}) for this object is replaced with the passed value
   * and the transactional view is marked as updated (see {@link StoreEntryTxView#updated}).
   *
   * @param key The key of the object to update.
   * @param value The updated object instance.
   * @throws JacisTransactionAlreadyPreparedForCommitException if the current transaction has already been prepared for commit
   */
  public void update(K key, TV value) throws JacisTransactionAlreadyPreparedForCommitException {
    JacisStoreTxView<K, TV, CV> txView = getOrCreateTxView().assertWritable();
    if (txView.isCommitPending()) {
      throw new JacisTransactionAlreadyPreparedForCommitException("Failed to update " + key + " because transaction is already prepared for commit: " + txView);
    }
    StoreEntryTxView<K, TV, CV> entryTxView = getOrCreateEntryTxView(txView, key);
    entryTxView.updateValue(value);
  }

  /**
   * Remove the object for the passed key from the store (first only in the transactional view of course).
   * The method is equivalent to simply calling the {@link #update(Object, Object)} method with a 'null' value.
   *
   * @param key The key of the object to remove.
   */
  public void remove(K key) {
    update(key, null);
  }

  /**
   * Refresh the object for the passed key from the committed values. Note that all earlier modifications in the current transaction are lost.
   * First the current transactional view (if updated or not) is discarded.
   * Afterwards a fresh copy of the current committed value is stored in the transactional view by calling the {@link #get(Object)} method.
   *
   * @param key The key of the object to refresh.
   * @return the object for the passed key refreshed from the committed values. Note that all earlier modifications in the current transaction are lost.
   */
  public TV refresh(K key) { // refresh with committed version -> discard all changes made by the current TX
    JacisStoreTxView<K, TV, CV> txView = getTxView();
    txView.removeTxViewEntry(key, true);
    return get(key);
  }

  /**
   * Refresh the object for the passed key from the committed values if the object is not marked as updated.
   * Note that all earlier modifications in the current transaction are lost if the object is not marked as updated.
   * First the current transactional view (if updated or not) is discarded.
   * Afterwards a fresh copy of the current committed value is stored in the transactional view by calling the {@link #get(Object)} method.
   *
   * @param key The key of the object to refresh.
   * @return the object for the passed key refreshed from the committed values if the object is not marked as updated.
   */
  public TV refreshIfNotUpdated(K key) { // if not updated: refresh with committed version -> discard all changes made by the current TX
    JacisStoreTxView<K, TV, CV> txView = getTxView();
    txView.removeTxViewEntry(key, false);
    return get(key);
  }

  /**
   * Returns the current size of the store.
   * Note that the size is not exact because all entries in the committed values are counted.
   * Since objects created or deleted in a pending transaction also have an entry with null value in the committed values
   * these objects are counted as well.
   * @return The current size of the store.
   */
  public int size() { // heuristic (due to concurrent access)
    return store.size();
  }

  /**
   * Execute the passed operation (without return value) as an atomic operation.
   * The execution of atomic operations can not overlap with the execution of other atomic operations
   * (but normal operations may overlap).
   * @param atomicOperation The operation to execute atomically
   */
  public void executeAtomic(Runnable atomicOperation) { // Execute an atomic operation. No internalCommit of any other TX and no other atomic action will interleave.
    withReadLock(runnableWrapper(atomicOperation));
  }

  /**
   * Execute the passed operation (with return value) as an atomic operation.
   * @param atomicOperation The operation to execute atomically
   * @param <R> The return type of the operation
   * @return The return value of the operation
   */
  public <R> R computeAtomic(Supplier<R> atomicOperation) { // Execute an atomic operation. No internalCommit of any other TX and no other atomic action will interleave.
    return withReadLock(atomicOperation);
  }

  /**
   * Accumulate a value from all objects with the passed accumulator function.
   * The accumulation starts with the initial value passed to the 'target' parameter.
   * For all objects the accumulator method is called with the current value of the target and the object.
   * Inside the accumulator method the target value is updated.
   * The objects are passed to the accumulator in read-only mode if supported.
   * The objects are collected by calling the {@link #getAllReadOnly(Predicate)} with 'null' as predicate.
   * <p>
   * Example (simply counting the objects):
   * <p>
   * ----
   * int objectCount = store.accumulate(new AtomicInteger(), (i,o)-&gt;i.incrementAndGet()).get();
   * ----
   *
   * @param target      The initial value for the target
   * @param accumulator The accumulator method getting the current value of the accumulation target (type 'C') and an object (type 'TV').
   * @param <C>         The type of the accumulation target.
   * @return The accumulation result.
   */
  public <C> C accumulate(C target, BiConsumer<C, TV> accumulator) {
    for (TV entryTxView : getAllReadOnly(null)) {
      accumulator.accept(target, entryTxView);
    }
    return target;
  }

  /**
   * Accumulate a value from all objects with the passed accumulator function as an atomic operation.
   * The method executes the {@link #accumulate(Object, BiConsumer)} method as an atomic operations.
   * Therefore this method is passed as functional parameter to the {@link #computeAtomic(Supplier)} method.
   * The execution of atomic operations can not overlap with the execution of other atomic operations (but normal operations may overlap).
   *
   * @param target      The initial value for the target
   * @param accumulator The accumulator method getting the current value of the accumulation target (type 'C') and an object (type 'TV').
   * @param <C>         The type of the accumulation target.
   * @return The accumulation result (computed as an atomic operation).
   */
  public <C> C accumulateAtomic(C target, BiConsumer<C, TV> accumulator) {
    return computeAtomic(() -> accumulate(target, accumulator));
  }

  /**
   * Returns the value that was valid as the object was first accessed by the current TX (null if untouched).
   *
   * @param key The key of the desired object.
   * @return the value that was valid as the object was first accessed by the current TX (null if untouched).
   */
  public TV getTransactionStartValue(K key) {
    assertTrackOriginalValue();
    JacisStoreTxView<K, TV, CV> txView = getTxView();
    StoreEntryTxView<K, TV, CV> entryTxView = txView == null ? null : txView.getEntryTxView(key);
    return entryTxView == null ? null : entryTxView.getOrigValue(); // if TX never touched the object we return null
  }

  /**
   * Returns a info object /type {@link StoreEntryInfo}) containing information regarding the current state of the object
   * (regarding the committed values and the current transactional view).
   *
   * @param key The key of the desired object.
   * @return a info object /type {@link StoreEntryInfo}) containing information regarding the current state of the object.
   */
  public StoreEntryInfo<K, TV, CV> getObjectInfo(K key) {
    JacisStoreTxView<K, TV, CV> txView = getTxView();
    StoreEntry<K, TV, CV> committedEntry = getCommittedEntry(key);
    StoreEntryTxView<K, TV, CV> entryTxView = txView == null ? null : txView.getEntryTxView(key);
    return new StoreEntryInfo<>(key, committedEntry, entryTxView, txView);
  }

  /**
   * Clear the complete store, remove all committed values and invalidate all pending transactions.
   */
  public synchronized void clear() {
    storeAccessLock.writeLock().lock();// <======= **WRITE** LOCK =====
    try {
      for (JacisStoreTxView<K, TV, CV> txCtx : txViewMap.values()) {
        txCtx.invalidate("store cleared");
      }
      store.clear();
      trackedViewRegistry.clearViews();
    } finally {
      storeAccessLock.writeLock().unlock();// <======= **WRITE** UNLOCK =====
    }
  }

  //======================================================================================
  // transaction demarcation methods
  //======================================================================================

  @Override
  protected void internalPrepare(JacisTransactionHandle transaction) {
    withWriteLock(runnableWrapper(() -> new StoreTxDemarcationExecutor().executePrepare(this, transaction)));
  }

  @Override
  protected void internalCommit(JacisTransactionHandle transaction) {
    withWriteLock(runnableWrapper(() -> new StoreTxDemarcationExecutor().executeCommit(this, transaction)));
  }

  @Override
  protected void internalRollback(JacisTransactionHandle transaction) {
    withWriteLock(runnableWrapper(() -> new StoreTxDemarcationExecutor().executeRollback(this, transaction)));
  }

  //======================================================================================
  // helper methods to deal with transaction views of entries
  //======================================================================================

  private StoreEntryTxView<K, TV, CV> getOrCreateEntryTxView(JacisStoreTxView<K, TV, CV> txView, K key) {
    StoreEntryTxView<K, TV, CV> entryTxView = txView.getEntryTxView(key);
    if (entryTxView == null) {
      entryTxView = withWriteLock(() -> txView.createTxViewEntry(getOrCreateCommittedEntry(key)));
    }
    return entryTxView;
  }

  //======================================================================================
  // helper methods to deal with committed entries
  //======================================================================================

  private StoreEntry<K, TV, CV> createCommittedEntry(K key) {
    StoreEntry<K, TV, CV> newCommittedEntry = new StoreEntry<>(this, key);
    StoreEntry<K, TV, CV> oldCommittedEntry = store.putIfAbsent(key, newCommittedEntry); // safe if another TX created one in the meantime
    return oldCommittedEntry != null ? oldCommittedEntry : newCommittedEntry;
  }

  private StoreEntry<K, TV, CV> getCommittedEntry(K key) {
    return store.get(key);
  }

  private StoreEntry<K, TV, CV> getOrCreateCommittedEntry(K key) {
    StoreEntry<K, TV, CV> committedEntry = store.get(key);
    if (committedEntry == null) {
      committedEntry = createCommittedEntry(key);
    }
    return committedEntry;
  }

  void checkRemoveCommittedEntry(StoreEntry<K, TV, CV> entryCommitted, JacisStoreTxView<K, TV, CV> currTxView) {
    if (entryCommitted.getValue() != null || entryCommitted.isLocked()) {
      return; // cannot remove
    }
    K key = entryCommitted.getKey();
    Collection<JacisStoreTxView<K, TV, CV>> txs;
    synchronized (txViewMap) {
      txs = new ArrayList<>(txViewMap.values());
    }
    for (JacisStoreTxView<K, TV, CV> txCtx : txs) {
      if (txCtx.isReadOnly()) {
        continue;
      } else if (currTxView.getTransaction().equals(txCtx.getTransaction())) {
        continue; // the current transaction referring a core entry can be ignored
      }
      if (txCtx.containsTxView(key)) {
        return; // still referred by transaction
      }
    }
    store.remove(key);
  }

  //======================================================================================
  // synchronized execution
  //======================================================================================

  private <R> R withWriteLock(Supplier<R> task) {
    storeAccessLock.writeLock().lock(); // <======= **WRITE** LOCK =====
    try {
      return task.get();
    } finally {
      storeAccessLock.writeLock().unlock(); // <======= **WRITE** UNLOCK =====
    }
  }

  private <R> R withReadLock(Supplier<R> task) {
    storeAccessLock.readLock().lock(); // <======= **WRITE** LOCK =====
    try {
      return task.get();
    } finally {
      storeAccessLock.readLock().unlock(); // <======= **WRITE** UNLOCK =====
    }
  }

  private Supplier<Object> runnableWrapper(Runnable r) {
    return () -> {
      r.run();
      return null;
    };
  }

  //======================================================================================
  // private methods to maintain the TX view
  //======================================================================================

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
    JacisStoreTxView<K, TV, CV> txView = txViewMap.get(transaction);
    if (txView == null && createIfAbsent) {
      txView = new JacisStoreTxView<>(this, transaction);
      txViewMap.put(transaction, txView);
    }
    return txView;
  }

  void notifyTxViewDestroyed(JacisStoreTxView<K, TV, CV> txView) {
    txViewMap.remove(txView.getTransaction());
  }

  //======================================================================================
  // other private helper methods
  //======================================================================================

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

  private static class KeyValuePair<PK, PV> {
    PK key;
    PV val;

    KeyValuePair(PK key, PV val) {
      this.key = key;
      this.val = val;
    }

  }

}
