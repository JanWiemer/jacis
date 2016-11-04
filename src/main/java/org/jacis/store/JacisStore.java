package org.jacis.store;

import org.jacis.container.JacisContainer;
import org.jacis.container.JacisObjectTypeSpec;
import org.jacis.exception.JacisStaleObjectException;
import org.jacis.exception.JacisTransactionAlreadyPreparedForCommitException;
import org.jacis.plugin.JacisModificationListener;
import org.jacis.plugin.objectadapter.JacisObjectAdapter;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Storing a single type of objects.
 *
 * All operations checking or returning entries of the store operate on the committed values merged with the
 * current transactional view (obtained with the currently active transaction handle from the map of transaction views).
 * This means that first the transactional view is checked if it contains an entry for the desired key.
 * If so this entry is returned, otherwise the committed value from the core store is returned.
 * Note that if an object is deleted in a transaction an entry with the value 'null' remains in the transactional view.
 * Therefore also deletions are properly handled with respect to isolation.
 *
 * @param <K> Key type of the store entry
 * @param <TV> Type of the objects in the transaction view. This is the type visible from the outside.
 * @author Jan Wiemer
 */
@SuppressWarnings({"unused"})
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
     *
     * @param withTxName transaction name used for the read only view.
     * @return a read only view of the current transaction context.
     */
    JacisReadOnlyTransactionContext createReadOnlyTransactionView(String withTxName);

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
     * Note that this method does not cause the referred object to be copied to thr transactional view.
     *
     * @param key The key of the entry to check.
     * @return if the object for the passed key has been updated in the current transaction.
     */
    boolean isUpdated(K key);

    /**
     * Returns if the object for the passed key is stale.
     * An object is considered to be stale if after first reading it in the current transaction,
     * an updated version of the same object has been committed by another transaction.
     * Note that this method does not cause the referred object to be copied to thr transactional view.
     * @param key The key of the entry to check.
     * @return if the object for the passed key is stale.
     */
    boolean isStale(K key);

    /**
     * Checks if the object for the passed key is stale and throws a {@link JacisStaleObjectException} if so.
     * An object is considered to be stale if after first reading it in the current transaction,
     * an updated version of the same object has been committed by another transaction.
     * Note that this method does not cause the referred object to be copied to thr transactional view.
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
     * Otherwise the behaviour depends on the object type:
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
     * @param key The key of the desired entry.
     * @param projection The projection function computing the desired return value (of the passed type 'P') from the object.
     * @param <P> The result type of the projection
     * @return a read only projection of the object for the passed value.
     */
    <P> P getProjectionReadOnly(K key, Function<TV, P> projection);

    /**
     * Returns a stream of all objects (not 'null') currently stored in the store.
     * Note that the method operates on the committed values merged with the current transactional view (see class description).
     * If the transactional view did not already contain an entry it is copied to the transactional view now.
     *
     * @return a stream of all objects (not 'null') currently stored in the store.
     */
    Stream<TV> stream();

    /**
     * Returns a stream of read only views for all objects (not 'null') currently stored in the store.
     * Note that the method operates on the committed values merged with the current transactional view (see class description).
     * Further note that the behavior of the method is equivalent to the behavior of the {@link #getReadOnly} method for a single object.
     *
     * @return a stream of all objects (not 'null') currently stored in the store.
     */
    Stream<TV> streamReadOnly();

    /**
     * Returns a stream of all objects (not 'null') currently stored in the store filtered by the passed filter.
     * Note that the method operates on the committed values merged with the current transactional view (see class description).
     * If supported the filter predicate is checked on a read only view of the object (without cloning it).
     * Only the objects passing the filter are is copied to the transactional view (if they are not yet contained there).
     *
     * @param filter a filter predicate deciding if an object should be contained in the resulting stream ('null' means all objects should be contained)
     * @return a stream of all objects (not 'null') currently stored in the store filtered by the passed filter.
     */
    Stream<TV> stream(Predicate<TV> filter);

    /**
     * Returns a stream of read only views for all objects (not 'null') currently stored in the store filtered by the passed filter.
     * Note that the method operates on the committed values merged with the current transactional view (see class description).
     * If supported the filter predicate is checked on a read only view of the object (without cloning it).
     * Further note that the behavior of the method is equivalent to the behavior of the {@link #getReadOnly} method for a single object
     * (only the objects passing the filter may be copied to the transactional view if no read only view is supported (and they are not yet contained there)).
     *
     * @param filter a filter predicate deciding if an object should be contained in the resulting stream ('null' means all objects should be contained)
     * @return a stream of all objects (not 'null') currently stored in the store filtered by the passed filter.
     */
    Stream<TV> streamReadOnly(Predicate<TV> filter);

    /**
     * Returns a list of all objects (not 'null') currently stored in the store.
     *
     * @return a list of all objects (not 'null') currently stored in the store.
     */
    List<TV> getAll();

    /**
     * Returns a list of all objects (not 'null') currently stored in the store filtered by the passed filter.
     * The method uses the {@link #stream(Predicate)} method and collects the results to a list.
     *
     * @param filter a filter predicate deciding if an object should be contained in the resulting stream ('null' means all objects should be contained)
     * @return a list of all objects (not 'null') currently stored in the store filtered by the passed filter.
     */
    List<TV> getAll(Predicate<TV> filter);

    /**
     * Returns a list of read-only views for all objects (not 'null') currently stored in the store.
     *
     * @return a list of read-only views for all objects (not 'null') currently stored in the store.
     */
    List<TV> getAllReadOnly();

    /**
     * Returns a list of read-only views for all objects (not 'null') currently stored in the store filtered by the passed filter.
     * The method uses the {@link #streamReadOnly(Predicate)} method and collects the results to a list.
     *
     * @param filter a filter predicate deciding if an object should be contained in the resulting stream ('null' means all objects should be contained)
     * @return a list of read-only views for all objects (not 'null') currently stored in the store filtered by the passed filter.
     */
    List<TV> getAllReadOnly(Predicate<TV> filter);

    /**
     * Returns a list of all objects (not 'null') currently stored in the store filtered by the passed filter.
     * The method executes the {@link #getAll(Predicate)} method as an atomic operations.
     * Therefore this method is passed as functional parameter to the {@link #computeAtomic(Supplier)} method.
     * The execution of atomic operations can not overlap with the execution of other atomic operations (but normal operations may overlap).
     *
     * @param filter a filter predicate deciding if an object should be contained in the resulting stream ('null' means all objects should be contained)
     * @return a list of all objects (not 'null') currently stored in the store filtered by the passed filter.
     */
    List<TV> getAllAtomic(Predicate<TV> filter);

    /**
     * Returns a list of read-only views for all objects (not 'null') currently stored in the store filtered by the passed filter.
     * The method executes the {@link #getAllReadOnly(Predicate)} method as an atomic operations.
     * Therefore this method is passed as functional parameter to the {@link #computeAtomic(Supplier)} method.
     * The execution of atomic operations can not overlap with the execution of other atomic operations (but normal operations may overlap).
     *
     * @param filter a filter predicate deciding if an object should be contained in the resulting stream ('null' means all objects should be contained)
     * @return a list of read-only views for all objects (not 'null') currently stored in the store filtered by the passed filter.
     */
    List<TV> getAllReadOnlyAtomic(Predicate<TV> filter);

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
    void update(K key, TV value) throws JacisTransactionAlreadyPreparedForCommitException;

    /**
     * Remove the object for the passed key from the store (first only in the transactional view of course).
     * The method is equivalent to simply calling the {@link #update(Object, Object)} method with a 'null' value.
     *
     * @param key The key of the object to remove.
     */
    void remove(K key);

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
     * Returns the current size of the store.
     * Note that the size is not exact because all entries in the committed values are counted.
     * Since objects created or deleted in a pending transaction also have an entry with null value in the committed values
     * these objects are counted as well.
     * @return The current size of the store.
     */
    int size();

    /**
     * Execute the passed operation (without return value) as an atomic operation.
     * The execution of atomic operations can not overlap with the execution of other atomic operations
     * (but normal operations may overlap).
     * @param atomicOperation The operation to execute atomically
     */
    void executeAtomic(Runnable atomicOperation);

    /**
     * Execute the passed operation (with return value) as an atomic operation.
     * @param atomicOperation The operation to execute atomically
     * @param <R> The return type of the operation
     * @return The return value of the operation
     */
    <R> R computeAtomic(Supplier<R> atomicOperation);

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
    <C> C accumulate(C target, BiConsumer<C, TV> accumulator);

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
    <C> C accumulateAtomic(C target, BiConsumer<C, TV> accumulator);

    /**
     * Returns the value that was valid as the object was first accessed by the current TX (null if untouched).
     *
     * @param key The key of the desired object.
     * @return the value that was valid as the object was first accessed by the current TX (null if untouched).
     */
    TV getTransactionStartValue(K key);

    /**
     * Returns a info object /type {@link StoreEntryInfo}) containing information regarding the current state of the object
     * (regarding the committed values and the current transactional view).
     *
     * @param key The key of the desired object.
     * @return a info object /type {@link StoreEntryInfo}) containing information regarding the current state of the object.
     */
    StoreEntryInfo<K, TV, ?> getObjectInfo(K key);

    /**
     * Clear the complete store, remove all committed values and invalidate all pending transactions.
     */
    void clear();
}
