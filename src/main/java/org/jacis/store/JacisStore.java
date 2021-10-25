/*
 * Copyright (c) 2017. Jan Wiemer
 */

package org.jacis.store;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.jacis.JacisApi;
import org.jacis.container.JacisContainer;
import org.jacis.container.JacisObjectTypeSpec;
import org.jacis.exception.JacisStaleObjectException;
import org.jacis.exception.JacisTransactionAlreadyPreparedForCommitException;
import org.jacis.plugin.JacisModificationListener;
import org.jacis.plugin.objectadapter.JacisObjectAdapter;

/**
 * Storing a single type of objects.
 * 
 * All operations checking or returning entries of the store operate on the committed values merged with the
 * current transactional view (obtained with the currently active transaction handle from the map of transaction views).
 * This means that first the transactional view is checked if it contains an entry for the desired key.
 * If so this entry is returned, otherwise the committed value from the core store is returned.
 * Note that if an object is deleted in a transaction an entry with the value <code>null</code> remains in the transactional view.
 * Therefore also deletions are properly handled with respect to isolation.
 * 
 * @param <K>  Key type of the store entry
 * @param <TV> Type of the objects in the transaction view. This is the type visible from the outside.
 * @author Jan Wiemer
 */
@JacisApi
public interface JacisStore<K, TV> {

  /** @return the reference to the JACIS container this store belongs to */
  JacisContainer getContainer();

  /** @return the store identifier uniquely identifying this store inside the container */
  JacisContainer.StoreIdentifier getStoreIdentifier();

  /** @return the object type specification for the objects stored in this store */
  JacisObjectTypeSpec<K, TV, ?> getObjectTypeSpec();

  /** @return the object adapter defining how to copy objects from the committed view to a transactional view and back */
  JacisObjectAdapter<TV, ?> getObjectAdapter();

  /** @return the list of listeners notified on each modification on the committed values in the store */
  List<JacisModificationListener<K, TV>> getModificationListeners();

  /**
   * Add the passed listener (implementing the interface {@link JacisModificationListener}).
   * The listener will be notified on each modification on the committed values in the store.
   *
   * @param listener the listener to notify
   * @return this store for method chaining
   */
  JacisStore<K, TV> registerModificationListener(JacisModificationListener<K, TV> listener);

  /** @return tte registry of tracked views for this store that are kept up to date on each commit automatically */
  TrackedViewRegistry<K, TV> getTrackedViewRegistry();

  /**
   * Create a read only view of the current transaction context that can be used (read only) in a different thread.
   * This can be used to share one single transaction view in several threads.
   * Before accessing the object store the other thread should set the returned context
   * with the method {@link #startReadOnlyTransactionWithContext(JacisReadOnlyTransactionContext)}.
   * Note: the returned context is threadsafe!
   *
   * @param withTxName transaction name used for the read only view.
   * @return a read only view of the current transaction context.
   */
  JacisReadOnlyTransactionContext createReadOnlyTransactionView(String withTxName);

  /**
   * Create a read only view of the current transaction context that can be used (read only) in a different thread.
   * This can be used to share one single transaction view in several threads.
   * Before accessing the object store the other thread should set the returned context
   * with the method {@link #startReadOnlyTransactionWithContext(JacisReadOnlyTransactionContext)}.
   * Note: the returned context is *not* threadsafe!
   *
   * @param withTxName transaction name used for the read only view.
   * @return a read only view of the current transaction context.
   */
  JacisReadOnlyTransactionContext createReadOnlyTransactionViewUnsafe(String withTxName);

  /**
   * Starts a new (read only) transaction with the passed transaction context.
   * The new transaction will work on a read only snapshot of the original transaction (where the context is obtained from).
   *
   * @param readOnlyTxContext the transaction context of the original transaction.
   */
  void startReadOnlyTransactionWithContext(JacisReadOnlyTransactionContext readOnlyTxContext);

  /**
   * Returns if the store contains an entry for the passed key.
   * Note that the method operates on the committed values merged with the current transactional view (see class description).
   *
   * @param key The key of the entry to check.
   * @return if the store contains an entry for the passed key.
   */
  boolean containsKey(K key);

  /**
   * Returns if the object for the passed key has been updated in the current transaction.
   * Note that an update has to be explicitly called for an object (by calling {@link #update(Object, Object)}).
   * The check returns true if there exists a transactional view
   * and the updated flag of this entry (see {@link StoreEntryTxView#updated}) is set (set by the 'update' method).
   * Note that this method does not cause the referred object to be copied to the transactional view.
   *
   * @param key The key of the entry to check.
   * @return if the object for the passed key has been updated in the current transaction.
   */
  boolean isUpdated(K key);

  /**
   * Returns if the object for the passed key is stale.
   * An object is considered to be stale if after first reading it in the current transaction,
   * an updated version of the same object has been committed by another transaction.
   * Note that this method does not cause the referred object to be copied to the transactional view.
   *
   * @param key The key of the entry to check.
   * @return if the object for the passed key is stale.
   */
  boolean isStale(K key);

  /**
   * Checks if the object for the passed key is stale and throws a {@link JacisStaleObjectException} if so.
   * An object is considered to be stale if after first reading it in the current transaction,
   * an updated version of the same object has been committed by another transaction.
   * Note that this method does not cause the referred object to be copied to the transactional view.
   *
   * @param key The key of the entry to check.
   * @throws JacisStaleObjectException thrown if the object for the passed key is stale.
   */
  void checkStale(K key) throws JacisStaleObjectException;

  /**
   * Returns the value for the passed key.
   * Note that the method operates on the committed values merged with the current transactional view (see class description).
   * If the transactional view did not already contain the entry for the key it is copied to the transactional view now.
   *
   * @param key The key of the desired entry.
   * @return the value for the passed key.
   */
  TV get(K key);

  /**
   * Returns the value for the passed key.
   * If the object is already stored in the transactional view of the current transaction this value is returned.
   * Otherwise the behavior depends on the object type:
   * If the object adapter for the store supports a read only mode, then a read only view on the committed value is returned.
   * Otherwise the committed entry for the key it is copied to the transactional view now.
   *
   * @param key The key of the desired entry.
   * @return the value for the passed key.
   */
  TV getReadOnly(K key);

  /**
   * Returns a read only projection of the object for the passed value.
   * First a read only view (if supported) of the object is obtained by the {@link #getReadOnly(Object)} method.
   * The projected is computed from the object by applying the passed projection function.
   *
   * @param key        The key of the desired entry.
   * @param projection The projection function computing the desired return value (of the passed type 'P') from the object.
   * @param <P>        The result type of the projection
   * @return a read only projection of the object for the passed value.
   */
  <P> P getProjectionReadOnly(K key, Function<TV, P> projection);

  /**
   * Returns a stream of all objects (not <code>null</code>) currently stored in the store.
   * Note that the method operates on the committed values merged with the current transactional view (see class description).
   * If the transactional view did not already contain an entry it is copied to the transactional view now.
   *
   * @return a stream of all objects (not <code>null</code>) currently stored in the store.
   */
  Stream<TV> stream();

  /**
   * Returns a stream of read only views for all objects (not <code>null</code>) currently stored in the store.
   * Note that the method operates on the committed values merged with the current transactional view (see class description).
   * Further note that the behavior of the method is equivalent to the behavior of the {@link #getReadOnly} method for a single object.
   *
   * @return a stream of all objects (not <code>null</code>) currently stored in the store.
   */
  Stream<TV> streamReadOnly();

  /**
   * Returns a stream of all objects (not <code>null</code>) currently stored in the store filtered by the passed filter.
   * Note that the method operates on the committed values merged with the current transactional view (see class description).
   * If supported the filter predicate is checked on a read only view of the object (without cloning it).
   * Only the objects passing the filter are is copied to the transactional view (if they are not yet contained there).
   *
   * @param filter a filter predicate deciding if an object should be contained in the resulting stream (<code>null</code> means all objects should be contained)
   * @return a stream of all objects (not <code>null</code>) currently stored in the store filtered by the passed filter.
   */
  Stream<TV> stream(Predicate<TV> filter);

  /**
   * Returns a stream of read only views for all objects (not <code>null</code>) currently stored in the store filtered by the passed filter.
   * Note that the method operates on the committed values merged with the current transactional view (see class description).
   * If supported the filter predicate is checked on a read only view of the object (without cloning it).
   * Further note that the behavior of the method is equivalent to the behavior of the {@link #getReadOnly} method for a single object
   * (only the objects passing the filter may be copied to the transactional view if no read only view is supported (and they are not yet contained there)).
   *
   * @param filter a filter predicate deciding if an object should be contained in the resulting stream (<code>null</code> means all objects should be contained)
   * @return a stream of all objects (not <code>null</code>) currently stored in the store filtered by the passed filter.
   */
  Stream<TV> streamReadOnly(Predicate<TV> filter);

  /**
   * Returns a list of all objects (not <code>null</code>) currently stored in the store.
   *
   * @return a list of all objects (not <code>null</code>) currently stored in the store.
   */
  List<TV> getAll();

  /**
   * Returns a list of all objects (not <code>null</code>) currently stored in the store filtered by the passed filter.
   * The method uses the {@link #stream(Predicate)} method and collects the results to a list.
   *
   * @param filter a filter predicate deciding if an object should be contained in the resulting list (<code>null</code> means all objects should be contained)
   * @return a list of all objects (not <code>null</code>) currently stored in the store filtered by the passed filter.
   */
  List<TV> getAll(Predicate<TV> filter);

  /**
   * Returns a list of read-only views for all objects (not <code>null</code>) currently stored in the store.
   *
   * @return a list of read-only views for all objects (not <code>null</code>) currently stored in the store.
   */
  List<TV> getAllReadOnly();

  /**
   * Returns a list of read-only views for all objects (not <code>null</code>) currently stored in the store filtered by the passed filter.
   * The method uses the {@link #streamReadOnly(Predicate)} method and collects the results to a list.
   *
   * @param filter a filter predicate deciding if an object should be contained in the resulting list (<code>null</code> means all objects should be contained)
   * @return a list of read-only views for all objects (not <code>null</code>) currently stored in the store filtered by the passed filter.
   */
  List<TV> getAllReadOnly(Predicate<TV> filter);

  /**
   * Returns a list of read-only views for all objects (not <code>null</code>) currently stored in the store.
   * The method is independent from an active transaction and takes a read only snapshot of the objects committed in the store.
   *
   * @return a list of read-only views for all committed objects (not <code>null</code>) currently stored in the store.
   */
  public List<TV> getReadOnlySnapshot();

  /**
   * Returns a list of read-only views for all objects (not <code>null</code>) currently stored in the store filtered by the passed filter.
   * The method is independent from an active transaction and takes a read only snapshot of the objects committed in the store.
   *
   * @param filter a filter predicate deciding if an object should be contained in the resulting list (<code>null</code> means all objects should be contained)
   * @return a list of read-only views for all committed objects (not <code>null</code>) currently stored in the store filtered by the passed filter.
   */
  public List<TV> getReadOnlySnapshot(Predicate<TV> filter);

  /**
   * Returns a list of all objects (not <code>null</code>) currently stored in the store filtered by the passed filter.
   * The method executes the {@link #getAll(Predicate)} method as an atomic operations.
   * Therefore this method is passed as functional parameter to the {@link #computeAtomic(Supplier)} method.
   * The execution of atomic operations can not overlap with the execution of a commit (changing the visible data) of another transaction (but normal operations on other transactions may overlap).
   *
   * @param filter a filter predicate deciding if an object should be contained in the resulting list (<code>null</code> means all objects should be contained)
   * @return a list of all objects (not <code>null</code>) currently stored in the store filtered by the passed filter.
   */
  List<TV> getAllAtomic(Predicate<TV> filter);

  /**
   * Returns a list of read-only views for all objects (not <code>null</code>) currently stored in the store filtered by the passed filter.
   * The method executes the {@link #getAllReadOnly(Predicate)} method as an atomic operations.
   * Therefore this method is passed as functional parameter to the {@link #computeAtomic(Supplier)} method.
   * The execution of atomic operations can not overlap with the execution of a commit (changing the visible data) of another transaction (but normal operations on other transactions may overlap).
   *
   * @param filter a filter predicate deciding if an object should be contained in the resulting list (<code>null</code> means all objects should be contained)
   * @return a list of read-only views for all objects (not <code>null</code>) currently stored in the store filtered by the passed filter.
   */
  List<TV> getAllReadOnlyAtomic(Predicate<TV> filter);

  /**
   * Returns a list of read-only views for all objects (not <code>null</code>) currently stored in the store.
   * The method is independent from an active transaction and takes a read only snapshot of the objects committed in the store.
   * The method executes the {@link #getReadOnlySnapshot()} method as an atomic operations
   * (using this method it is possible to get a snapshot without 'phantom reads').
   * . * Therefore this method is passed as functional parameter to the {@link #computeAtomic(Supplier)} method.
   * The execution of atomic operations can not overlap with the execution of a commit (changing the visible data) of another transaction (but normal operations on other transactions may overlap).
   *
   * @return a list of read-only views for all objects (not <code>null</code>) currently stored in the store.
   */
  public List<TV> getReadOnlySnapshotAtomic();

  /**
   * Returns a list of read-only views for all objects (not <code>null</code>) currently stored in the store filtered by the passed filter.
   * The method is independent from an active transaction and takes a read only snapshot of the objects committed in the store.
   * The method executes the {@link #getReadOnlySnapshot(Predicate)} method as an atomic operations
   * (using this method it is possible to get a snapshot without 'phantom reads').
   * Therefore this method is passed as functional parameter to the {@link #computeAtomic(Supplier)} method.
   * The execution of atomic operations can not overlap with the execution of a commit (changing the visible data) of another transaction (but normal operations on other transactions may overlap).
   *
   * @param filter a filter predicate deciding if an object should be contained in the resulting list (<code>null</code> means all objects should be contained)
   * @return a list of read-only views for all objects (not <code>null</code>) currently stored in the store filtered by the passed filter.
   */
  public List<TV> getReadOnlySnapshotAtomic(Predicate<TV> filter);

  /**
   * Helper method to get a paging access to the elements (read only versions) stored in the store.
   * First the elements are filtered and sorted according to the passed predicate and comparator,
   * afterwards the desired page is extracted according to the passed offset and page size.
   *
   * @param filter     a filter predicate deciding if an object should be contained in the paged date (<code>null</code> means all objects should be contained)
   * @param comparator a comparator to sort the object.
   * @param offset     The offset of the desired page in the sorted and filtered list of objects.
   * @param pageSize   The size of the desired page.
   * @return the page of n (=pageSize) objects starting at the offset in the filtered and sorted list
   */
  List<TV> getPageReadOnly(Predicate<TV> filter, Comparator<TV> comparator, long offset, long pageSize);

  /**
   * Helper method to get a paging access to the elements (read only versions) stored in the store.
   * The method returns wrapped (or transformed) versions of the objects.
   * This way the information in the objects can e.g. be enriched by additional information.
   * First the elements are filtered and sorted according to the passed predicate and comparator,
   * afterwards the desired page is extracted according to the passed offset and page size.
   *
   * @param wrapper    a function used to wrap a (read only) object before filtering, sorting and returning them
   * @param filter     a filter predicate deciding if an object should be contained in the paged date (<code>null</code> means all objects should be contained)
   * @param comparator a comparator to sort the object.
   * @param offset     The offset of the desired page in the sorted and filtered list of objects.
   * @param pageSize   The size of the desired page.
   * @param <PV>       The type of the wrapper object used in the returned page
   * @return the page of n (=pageSize) wrapped objects starting at the offset in the filtered and sorted list
   */
  <PV> List<PV> getWrapperPageReadOnly(Function<TV, PV> wrapper, Predicate<PV> filter, Comparator<PV> comparator, long offset, long pageSize);

  /**
   * Update the object for the passed key with the passed object value.
   * Note that the passed object instance may be the same (modified) instance obtained from the store before,
   * but also can be another instance.
   * Internally the value of the transactional view (see {@link StoreEntryTxView#txValue}) for this object is replaced with the passed value
   * and the transactional view is marked as updated (see {@link StoreEntryTxView#updated}).
   *
   * @param key   The key of the object to update.
   * @param value The updated object instance.
   * @throws JacisTransactionAlreadyPreparedForCommitException if the current transaction has already been prepared for commit
   */
  void update(K key, TV value) throws JacisTransactionAlreadyPreparedForCommitException;

  /**
   * This method updates or inserts all passed values as a bulk update / insert.
   * The keys for the objects is computed using the mandatory keyExtractor function.
   * The functionality is similar to the update method for a single object (see {@link #update(Object, Object)}).
   * 
   * @param values       The set of objects to update or insert.
   * @param keyExtractor The function computing the key for each object to update or insert.
   * @throws JacisTransactionAlreadyPreparedForCommitException if the current transaction has already been prepared for commit
   */
  void update(Collection<TV> values, Function<TV, K> keyExtractor) throws JacisTransactionAlreadyPreparedForCommitException;

  /**
   * Remove the object for the passed key from the store (first only in the transactional view of course).
   * The method is equivalent to simply calling the {@link #update(Object, Object)} method with a <code>null</code> value.
   *
   * @param key The key of the object to remove.
   * @throws JacisTransactionAlreadyPreparedForCommitException if the current transaction has already been prepared for commit
   */
  void remove(K key) throws JacisTransactionAlreadyPreparedForCommitException;

  /**
   * Refresh the object for the passed key from the committed values. Note that all earlier modifications in the current transaction are lost.
   * First the current transactional view (if updated or not) is discarded.
   * Afterwards a fresh copy of the current committed value is stored in the transactional view by calling the {@link #get(Object)} method.
   *
   * @param key The key of the object to refresh.
   * @return the object for the passed key refreshed from the committed values. Note that all earlier modifications in the current transaction are lost.
   */
  TV refresh(K key);

  /**
   * Refresh the object for the passed key from the committed values if the object is not marked as updated.
   * Note that all earlier modifications in the current transaction are lost if the object is not marked as updated.
   * First the current transactional view (if updated or not) is discarded.
   * Afterwards a fresh copy of the current committed value is stored in the transactional view by calling the {@link #get(Object)} method.
   *
   * @param key The key of the object to refresh.
   * @return the object for the passed key refreshed from the committed values if the object is not marked as updated.
   */
  TV refreshIfNotUpdated(K key);

  /**
   * Initialize the store with the passed entries.
   * The actual key and value inserted into the store is computed by the passed extractor functions.
   * Note that the method initializes the store in a non transactional manner.
   * The store has to be empty before. During initialization all commits are blocked.
   * By passing the number of threads that shall insert the passed values
   * 
   * @param entries        The entries from which the store is initialized.
   * @param keyExtractor   Method to extract the key from an entry.
   * @param valueExtractor Method to extract the value from an entry.
   * @param <ST>           The type of the entries
   * @param nThreads       Number of threads to use for multythreaded inserts.
   */
  public <ST> void initStoreNonTransactional(List<ST> entries, Function<ST, K> keyExtractor, Function<ST, TV> valueExtractor, int nThreads);

  /**
   * Initialize the store with the passed values.
   * The key is computed by the passed extractor function.
   * Note that the method initializes the store in a non transactional manner.
   * The store has to be empty before. During initialization all commits are blocked.
   * By passing the number of threads that shall insert the passed values
   * 
   * @param values       The values the store is initialized with.
   * @param keyExtractor Method to extract the key from a value.
   * @param nThreads     Number of threads to use for multythreaded inserts.
   */
  public void initStoreNonTransactional(List<TV> values, Function<TV, K> keyExtractor, int nThreads);

  /**
   * Initialize the store with the passed key-value pairs.
   * Note that the method initializes the store in a non transactional manner.
   * The store has to be empty before. During initialization all commits are blocked.
   * By passing the number of threads that shall insert the passed values
   * 
   * @param entries  The entries (key value pairs) from which the store is initialized.
   * @param nThreads Number of threads to use for multythreaded inserts.
   */
  public void initStoreNonTransactional(List<KeyValuePair<K, TV>> entries, int nThreads);

  /**
   * Returns the current size of the store.
   * Note that the size is not exact because all entries in the committed values are counted.
   * Since objects created or deleted in a pending transaction also have an entry with null value in the committed values
   * these objects are counted as well.
   *
   * @return The current size of the store.
   */
  int size();

  /**
   * Execute the passed operation (without return value) as an atomic operation.
   * The execution of atomic operations can not overlap with the execution of a commit (changing the visible data) of another transaction (but normal operations on other transactions may overlap).
   * Note that this operation ensures only to be atomic for the current store. It does not guarantee that e.g. simultaneously a commit for another store is done.
   *
   * @param atomicOperation The operation to execute atomically
   */
  void executeAtomic(Runnable atomicOperation);

  /**
   * Execute the passed operation (with return value) as an atomic operation.
   * The execution of atomic operations can not overlap with the execution of a commit (changing the visible data) of another transaction (but normal operations on other transactions may overlap).
   * Note that this operation ensures only to be atomic for the current store. It does not guarantee that e.g. simultaneously a commit for another store is done.
   *
   * @param atomicOperation The operation to execute atomically
   * @param <R>             The return type of the operation
   * @return The return value of the operation
   */
  <R> R computeAtomic(Supplier<R> atomicOperation);

  /**
   * Execute the passed operation (without return value) as a global atomic operation (atomic over all stores).
   * The execution of global atomic operations can not overlap with the execution of a commit (changing the visible data) of another transaction (but normal operations on other transactions may overlap),
   * even if the commit is (currently) executed for any other store belonging to the same JACIS container.
   *
   * @param atomicOperation The operation to execute atomically
   */
  void executeGlobalAtomic(Runnable atomicOperation);

  /**
   * Execute the passed operation (with return value) as an global atomic operation (atomic over all stores).
   * The execution of global atomic operations can not overlap with the execution of a commit (changing the visible data) of another transaction (but normal operations on other transactions may overlap),
   * even if the commit is (currently) executed for any other store belonging to the same JACIS container.
   *
   * @param atomicOperation The operation to execute atomically
   * @param <R>             The return type of the operation
   * @return The return value of the operation
   */
  <R> R computeGlobalAtomic(Supplier<R> atomicOperation);

  /**
   * Accumulate a value from all objects with the passed accumulator function.
   * The accumulation starts with the initial value passed to the 'target' parameter.
   * For all objects the accumulator method is called with the current value of the target and the object.
   * Inside the accumulator method the target value is updated.
   * The objects are passed to the accumulator in read-only mode if supported.
   * The objects are collected by calling the {@link #getAllReadOnly(Predicate)} with <code>null</code> as predicate.
   * <p>
   * Example (simply counting the objects):
   * </p>
   * 
   * <pre>
   * int objectCount = store.accumulate(new AtomicInteger(), (i, o) -&gt; i.incrementAndGet()).get();
   * </pre>
   *
   * @param target      The initial value for the target
   * @param accumulator The accumulator method getting the current value of the accumulation target (type 'C') and an object (type 'TV').
   * @param <C>         The type of the accumulation target.
   * @return The accumulation result.
   */
  <C> C accumulate(C target, BiConsumer<C, TV> accumulator);

  /**
   * Accumulate a value from all objects with the passed accumulator function as an atomic operation.
   * The method executes the {@link #accumulate(Object, BiConsumer)} method as an atomic operations.
   * Therefore this method is passed as functional parameter to the {@link #computeAtomic(Supplier)} method.
   * The execution of atomic operations can not overlap with the execution of a commit (changing the visible data) of another transaction (but normal operations on other transactions may overlap).
   *
   * @param target      The initial value for the target
   * @param accumulator The accumulator method getting the current value of the accumulation target (type 'C') and an object (type 'TV').
   * @param <C>         The type of the accumulation target.
   * @return The accumulation result (computed as an atomic operation).
   */
  <C> C accumulateAtomic(C target, BiConsumer<C, TV> accumulator);

  /**
   * Returns the value that was valid as the object was first accessed by the current TX (null if untouched).
   *
   * @param key The key of the desired object.
   * @return the value that was valid as the object was first accessed by the current TX (null if untouched).
   */
  TV getTransactionStartValue(K key);

  /**
   * Returns if there is a transaction local view existing for the passed key in the current transaction.
   * 
   * @param key The key of the desired object.
   * @return if there is a transaction local view existing for the passed key in the current transaction.
   */
  boolean hasTransactionView(K key);

  /**
   * Returns the original version object at the point of time it was cloned to the transactional view of the object
   * 
   * @param key The key of the desired object.
   * @return the original version or the transactional view of the object (version of the object at the point of time it was cloned to the transaction view).
   */
  public long getTransactionViewVersion(K key);

  /**
   * Returns a info object (type {@link StoreEntryInfo}) containing information regarding the current state of the object
   * (regarding the committed values and the current transactional view).
   *
   * @param key The key of the desired object.
   * @return a info object (type {@link StoreEntryInfo}) containing information regarding the current state of the object.
   */
  StoreEntryInfo<K, TV> getObjectInfo(K key);

  /**
   * Clear the complete store, remove all committed values and invalidate all pending transactions.
   */
  void clear();
}
