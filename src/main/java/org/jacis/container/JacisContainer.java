/*
 * Copyright (c) 2017. Jan Wiemer
 */

package org.jacis.container;

import jdk.jfr.FlightRecorder;
import org.jacis.JacisApi;
import org.jacis.exception.*;
import org.jacis.plugin.JacisTransactionListener;
import org.jacis.plugin.persistence.JacisPersistenceAdapter;
import org.jacis.plugin.txadapter.JacisTransactionAdapter;
import org.jacis.plugin.txadapter.local.JacisLocalTransaction;
import org.jacis.plugin.txadapter.local.JacisTransactionAdapterLocal;
import org.jacis.store.*;
import org.jacis.util.TransactionExecutor;

import java.util.*;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

//       _   _    ____ ___ ____
//      | | / \  / ___|_ _/ ___|
//   _  | |/ _ \| |    | |\___ \
//  | |_| / ___ \ |___ | | ___) |
//   \___/_/   \_\____|___|____/
//

/**
 * The Jacis container holding the stores for the different object types.
 * <p>
 * The <em>JacisContainer</em> is the main class of the Java ACI Store.
 * The container stores a number of individual stores for different object types.
 * Transactions are managed by the container and are valid for all stores in the container.
 * This class provides methods to create stores for different object types and provides access to those stores.
 *
 * Note that the stores created inside a container are organized in namespaces.
 * In a single namespace there can be only one store for a given pair of key- and value-classes.
 *
 * @author Jan Wiemer
 */
@SuppressWarnings("unused") // since this is an API of the library
@JacisApi
public class JacisContainer {

  public static final String DEFAULT_STORE_NAMESPACE = "DEFAULT";

  /** ThreadLocal storing the transaction info object for the last finished transaction */
  private final static ThreadLocal<JacisTransactionInfo> lastFinishedTransactionInfo = new ThreadLocal<>();

  /** {@link JacisTransactionAdapter} to bind the Jacis Store to externally managed transactions. */
  private final JacisTransactionAdapter txAdapter;
  /** Map assigning the stores (values of type {@link JacisStoreImpl}) to the store identifiers (keys of type {@link StoreIdentifier}). */
  private final Map<StoreIdentifier, JacisStore<?, ?>> storeMap = new ConcurrentHashMap<>();
  /** List of transaction listeners / observers (type {@link JacisTransactionListener}) providing call-backs before / after prepare / internalCommit / rollback. */
  private final List<JacisTransactionListener> txListeners = new CopyOnWriteArrayList<>();
  /** List of registered JFR periodic event hooks (stored to de-register at destroy) */
  private final List<Runnable> periodicJfrEventHooks = new ArrayList<>();
  /** Lock object to synchronize the TX demarcation operations (prepare / commit / rollback) over all threads and stores. */
  private final ReadWriteLock transactionDemarcationLock = new ReentrantReadWriteLock(true);

  /**
   * Create a container with the passed transaction adapter.
   *
   * @param txAdapter The transaction adapter binding the container to externally managed transactions
   */
  public JacisContainer(JacisTransactionAdapter txAdapter) {
    this.txAdapter = txAdapter;
  }

  /**
   * Create a container with a transaction adapter binding the store to locally managed transactions.
   * Transactions have to be started explicitly by calling the method {@link #beginLocalTransaction(String description)} in this case
   */
  public JacisContainer() {
    this(new JacisTransactionAdapterLocal());
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(#stores=" + storeMap.size() + ")";
  }

  /**
   * Register the passed transaction listener ({@link JacisTransactionListener}).
   * All registered listeners will be informed before / after each prepare / internalCommit / rollback of any transaction on this container.
   *
   * @param listener The transaction listener to register.
   * @return This container itself for method chaining.
   */
  public JacisContainer registerTransactionListener(JacisTransactionListener listener) {
    if (!txListeners.contains(listener)) {
      txListeners.add(listener);
    }
    return this;
  }

  /**
   * Create a store in the default namespace for the passed object type specification (type {@link JacisObjectTypeSpec}).
   * The passed specification determines the type of the keys and the type of the values stored in the created store.
   *
   * @param objectTypeSpec object type specification describing the objects to be stored.
   * @param <K>            Key type of the store entry
   * @param <TV>           Type of the objects in the transaction view. This is the type visible from the outside.
   * @param <CV>           Type of the objects as they are stored in the internal map of committed values. This type is not visible from the outside.
   * @return A reference to the created store (type {@link JacisStoreImpl})
   */
  public <K, TV, CV> JacisStoreAdminInterface<K, TV, CV> createStore(JacisObjectTypeSpec<K, TV, CV> objectTypeSpec) {
    return createStore(DEFAULT_STORE_NAMESPACE, objectTypeSpec);
  }

  /**
   * Create a store in the given namespace for the passed object type specification (type {@link JacisObjectTypeSpec}).
   * The passed specification determines the type of the keys and the type of the values stored in the created store.
   *
   * @param objectTypeSpec object type specification describing the objects to be stored.
   * @param storeNamespace The namespace where ths store lives. In one namespace there may only be one store for a key and value class combination.
   * @param <K>            Key type of the store entry
   * @param <TV>           Type of the objects in the transaction view. This is the type visible from the outside.
   * @param <CV>           Type of the objects as they are stored in the internal map of committed values. This type is not visible from the outside.
   * @return A reference to the created store (type {@link JacisStoreImpl})
   */
  public <K, TV, CV> JacisStoreAdminInterface<K, TV, CV> createStore(String storeNamespace, JacisObjectTypeSpec<K, TV, CV> objectTypeSpec) {
    StoreIdentifier storeIdentifier = new StoreIdentifier(storeNamespace, objectTypeSpec.getKeyClass(), objectTypeSpec.getValueClass());
    JacisStoreImpl<K, TV, CV> store = new JacisStoreImpl<>(this, storeIdentifier, objectTypeSpec);
    JacisPersistenceAdapter<K, TV> persistenceAdapter = objectTypeSpec.getPersistenceAdapter();
    if (persistenceAdapter != null) {
      persistenceAdapter.initializeStore(store);
      store.registerModificationListener(persistenceAdapter);
      registerTransactionListener(persistenceAdapter);
    }
    storeMap.put(storeIdentifier, store);
    transactionDemarcationLock.writeLock().lock();
    try {
      Runnable jfrEvtHook = () -> new EventsJfr.JacisStoreStatisticJfrEvent(store);
      periodicJfrEventHooks.add(jfrEvtHook);
      FlightRecorder.addPeriodicEvent(EventsJfr.JacisStoreStatisticJfrEvent.class, jfrEvtHook);
    } finally {
      transactionDemarcationLock.writeLock().unlock();
    }
    return store;
  }

  /**
   * Get the store (type {@link JacisStore}) for the passed key and value type from the default namespace.
   *
   * @param keyClass       Class of the keys that should be stored in the searched store
   * @param valueClass     Class of the values that should be stored in the searched store
   * @param <K>            Key type of the store entry
   * @param <TV>           Type of the objects in the transaction view. This is the type visible from the outside.
   * @param <CV>           Type of the objects as they are stored in the internal map of committed values. This type is not visible from the outside.
   * @return A reference to the found store (type {@link JacisStore}) (null if not found)
   */
  @SuppressWarnings("unchecked")
  public <K, TV, CV> JacisStore<K, TV> getStore(Class<K> keyClass, Class<TV> valueClass) {
    return this.getStore(DEFAULT_STORE_NAMESPACE, keyClass, valueClass);
  }

  /**
   * Get the store (type {@link JacisStore}) for the passed key and value type.
   *
   * @param storeNamespace The namespace where ths store lives. In one namespace there may only be one store for a key and value class combination.
   * @param keyClass       Class of the keys that should be stored in the searched store
   * @param valueClass     Class of the values that should be stored in the searched store
   * @param <K>            Key type of the store entry
   * @param <TV>           Type of the objects in the transaction view. This is the type visible from the outside.
   * @param <CV>           Type of the objects as they are stored in the internal map of committed values. This type is not visible from the outside.
   * @return A reference to the found store (type {@link JacisStore}) (null if not found)
   */
  @SuppressWarnings("unchecked")
  public <K, TV, CV> JacisStore<K, TV> getStore(String storeNamespace, Class<K> keyClass, Class<TV> valueClass) {
    StoreIdentifier storeIdentifier = new StoreIdentifier(storeNamespace, keyClass, valueClass);
    return (JacisStoreImpl<K, TV, CV>) storeMap.get(storeIdentifier);
  }

  /**
   * Get all stores (type {@link JacisStore}) matching the passed filter.
   *
   * @param filter Filter for the list of stores that should be returned.
   * @return A List of all stores (type {@link JacisStore}) matching the passed filter.
   */
  public Collection<JacisStore<?, ?>> getAllStores(Predicate<JacisStore<?, ?>> filter) {
    if (filter != null) {
      return storeMap.values().stream().filter(filter).collect(Collectors.toList());
    } else {
      return storeMap.values();
    }
  }

  /**
   * Get all stores (type {@link JacisStore}).
   *
   * @return A List of all stores (type {@link JacisStore}).
   */
  public Collection<JacisStore<?, ?>> getAllStores() {
    return getAllStores(null);
  }

  /**
   * Get the store (type {@link JacisStoreAdminInterface}) for the passed key and value type.
   *
   * @param keyClass   Class of the keys that should be stored in the searched store
   * @param valueClass Class of the values that should be stored in the searched store
   * @param <K>        Key type of the store entry
   * @param <TV>       Type of the objects in the transaction view. This is the type visible from the outside.
   * @param <CV>       Type of the objects as they are stored in the internal map of committed values. This type is not visible from the outside.
   * @return A reference to the found store (type {@link JacisStoreAdminInterface}) (null if not found)
   */
  @SuppressWarnings("unchecked")
  public <K, TV, CV> JacisStoreAdminInterface<K, TV, CV> getStoreAdminInterface(Class<K> keyClass, Class<TV> valueClass) {
    return getStoreAdminInterface(DEFAULT_STORE_NAMESPACE, keyClass, valueClass);
  }

  /**
   * Get the store (type {@link JacisStoreAdminInterface}) for the passed key and value type.
   *
   * @param storeNamespace The namespace where ths store lives. In one namespace there may only be one store for a key and value class combination.
   * @param keyClass       Class of the keys that should be stored in the searched store
   * @param valueClass     Class of the values that should be stored in the searched store
   * @param <K>            Key type of the store entry
   * @param <TV>           Type of the objects in the transaction view. This is the type visible from the outside.
   * @param <CV>           Type of the objects as they are stored in the internal map of committed values. This type is not visible from the outside.
   * @return A reference to the found store (type {@link JacisStoreAdminInterface}) (null if not found)
   */
  @SuppressWarnings("unchecked")
  public <K, TV, CV> JacisStoreAdminInterface<K, TV, CV> getStoreAdminInterface(String storeNamespace, Class<K> keyClass, Class<TV> valueClass) {
    StoreIdentifier storeIdentifier = new StoreIdentifier(storeNamespace, keyClass, valueClass);
    return (JacisStoreImpl<K, TV, CV>) storeMap.get(storeIdentifier);
  }

  /**
   * Clear all stored objects in all stores in this container.
   * The stores are cleared independently of any transaction context.
   * It is not necessary to start a transaction to call this method.
   * All ending transactions are invalidated, that means any attempt to prepare or internalCommit them is ignored (without an exception).
   */
  public void clearAllStores() {
    transactionDemarcationLock.writeLock().lock();
    try {
      storeMap.values().forEach(JacisStore::clear);
    } finally {
      transactionDemarcationLock.writeLock().unlock();
    }
  }

  public void destroy() {
    transactionDemarcationLock.writeLock().lock();
    try {
      storeMap.values().forEach(JacisStore::clear);
      periodicJfrEventHooks.forEach(FlightRecorder::removePeriodicEvent);
      periodicJfrEventHooks.clear();
    } finally {
      transactionDemarcationLock.writeLock().unlock();
    }
  }

  /** @return If for the current thread there is a transaction active on this container. */
  public boolean isInTransaction() {
    return txAdapter.isTransactionActive();
  }

  /**
   * Start a locally managed transactions on the container.
   * The transaction is valid for all stores in the container.
   * Note that a locally managed transaction can only be started if the container
   * has been initialized with a transaction adapter for locally managed transactions
   * (calling the constructor {@link #JacisContainer()}. Otherwise, an {@link IllegalStateException} is thrown.
   * The returned object represents the started transaction and provides method to internalCommit or rollback the transaction.
   * Note that each transaction is started with a description for logging and monitoring.
   * It is recommended to pass an explicit description by calling the method {@link #beginLocalTransaction(String)} ).
   * This convenience method tries to compute a default description by analyzing the call stack to determine the calling method.
   *
   * @return An object representing the stated transaction (type {@link JacisLocalTransaction})
   * @throws IllegalStateException if the container was not initialized with transaction adapter for locally managed transactions.
   */
  public JacisLocalTransaction beginLocalTransaction() throws IllegalStateException {
    return beginLocalTransaction(null);
  }

  /**
   * Start a locally managed transactions on the container.
   * The transaction is valid for all stores in the container.
   * Note that a locally managed transaction can only be started if the container
   * has been initialized with a transaction adapter for locally managed transactions
   * (calling the constructor {@link #JacisContainer()}).
   * Otherwise, an {@link IllegalStateException} is thrown.
   * The returned object represents the started transaction and provides method to internalCommit or rollback the transaction.
   *
   * @param description a description of the transaction for logging and monitoring
   * @return An object representing the stated transaction (type {@link JacisLocalTransaction})
   * @throws IllegalStateException if the container was not initialized with transaction adapter for locally managed transactions.
   */
  public JacisLocalTransaction beginLocalTransaction(String description) throws IllegalStateException {
    if (description == null) {
      description = Stream.of(new Exception("-").getStackTrace()) // go through the stack trace elements
          .filter(se -> !getClass().getName().equals(se.getClassName())) // ignore all stack trace elements for this class
          .filter(se -> !TransactionExecutor.class.getName().equals(se.getClassName())) // ignore all stack trace elements for this class
          .map(StackTraceElement::toString).findFirst().orElse("-"); // use the first from outside (the calling method) as description
    }
    if (txAdapter instanceof JacisTransactionAdapterLocal) {
      JacisTransactionAdapterLocal txAdapterLocal = (JacisTransactionAdapterLocal) txAdapter;
      lastFinishedTransactionInfo.remove();
      return txAdapterLocal.startLocalTransaction(this, description);
    } else {
      throw new IllegalStateException("Local transactions not supported! Local transactions need TX adapter " + JacisTransactionAdapterLocal.class.getSimpleName() + " but the configured is: " + txAdapter.getClass().getSimpleName());
    }
  }

  /**
   * @return a customizable helper class to execute transaction with retry logic.
   */
  public TransactionExecutor withLocalTx() {
    return new TransactionExecutor(this);
  }

  /**
   * Helper method executing the passed task (a {@link Runnable} usually passed as a lambda expression) within a locally started transaction.
   * First a locally managed transaction is started.
   * Then the passed task is executed.
   * If execution of the task succeeds the transaction is committed.
   * In case of any exception the transaction is rolled back.
   *
   * @param task     The task to execute inside a locally managed transaction
   * @param taskName A name of the task to execute (used for logging and exception messages...).
   * @throws IllegalStateException if the container was not initialized with transaction adapter for locally managed transactions.
   */
  public void withLocalTx(Runnable task, String taskName) throws IllegalStateException {
    TransactionExecutor.singleAttempt(this).execute(task, taskName);
  }

  public void withLocalTx(Runnable task) throws IllegalStateException {
    withLocalTx(task, null);
  }

  /**
   * Helper method executing the passed task (a {@link Supplier} (returning a result)
   * usually passed as a lambda expression) within a locally started transaction.
   * First a locally managed transaction is started.
   * Then the passed task is executed.
   * If execution of the task succeeds the transaction is committed.
   * In case of any exception the transaction is rolled back.
   * The result of the passed task is returned by the method.
   *
   * @param <R>      class / type of the returned result
   * @param task     The task to execute inside a locally managed transaction
   * @param taskName A name of the task to execute (used for logging and exception messages...).
   * @return the result of the passed task
   * @throws IllegalStateException if the container was not initialized with transaction adapter for locally managed transactions.
   */
  public <R> R withLocalTx(Supplier<R> task, String taskName) throws IllegalStateException {
    return TransactionExecutor.singleAttempt(this).execute(task, taskName);
  }

  public <R> R withLocalTx(Supplier<R> task) throws IllegalStateException {
    return withLocalTx(task, null);
  }

  /**
   * Helper method executing the passed task (a {@link Runnable} usually passed as a lambda expression) within a locally started transaction.
   * If another transaction has concurrently updated an object that should be updated in this transaction as well a {@link JacisStaleObjectException} is thrown.
   * This method catches this exception and retries to execute the passed task inside a new transaction for the passed number of attempts.
   * If the {@link JacisStaleObjectException} is thrown repeatedly for all these attempts the exception is propagated to the caller.
   * In case of any other exception the transaction is rolled back and the exception is propagated to the caller immediately.
   *
   * @param retries  Number of retries if transaction failed with {@link JacisStaleObjectException}
   * @param task     The task to execute inside a locally managed transaction
   * @param taskName A name of the task to execute (used for logging and exception messages...).
   * @throws IllegalStateException if the container was not initialized with transaction adapter for locally managed transactions.
   */
  public void withLocalTxAndRetry(int retries, Runnable task, String taskName) {
    TransactionExecutor.withRetries(this, retries).execute(task, taskName);
  }

  public void withLocalTxAndRetry(int retries, Runnable task) {
    withLocalTxAndRetry(retries, task, null);
  }

  /**
   * Helper method executing the passed task (a {@link Supplier} (returning a result)
   * usually passed as a lambda expression) within a locally started transaction.
   * If another transaction has concurrently updated an object that should be updated in this transaction as well a {@link JacisStaleObjectException} is thrown.
   * This method catches this exception and retries to execute the passed task inside a new transaction for the passed number of attempts.
   * If the {@link JacisStaleObjectException} is thrown repeatedly for all these attempts the exception is propagated to the caller.
   * In case of any other exception the transaction is rolled back and the exception is propagated to the caller immediately.
   * The result of the passed task is returned by the method.
   *
   * @param <R>      class / type of the returned result
   * @param retries  Number of retries if transaction failed with {@link JacisStaleObjectException}
   * @param task     The task to execute inside a locally managed transaction
   * @param taskName A name of the task to execute (used for logging and exception messages...).
   * @return the result of the passed task
   * @throws IllegalStateException if the container was not initialized with transaction adapter for locally managed transactions.
   */
  public <R> R withLocalTxAndRetry(int retries, Supplier<R> task, String taskName) {
    return TransactionExecutor.withRetries(this, retries).execute(task, taskName);
  }

  public <R> R withLocalTxAndRetry(int retries, Supplier<R> task) {
    return withLocalTxAndRetry(retries, task, null);
  }

  /**
   * This method returns a handle for the (local or global) transaction currently associated with the store for the current thread.
   * The passed flag determines if a {@link JacisNoTransactionException} is thrown when no transaction is active for the current thread.
   * Otherwise, the method simply returns null in case of a missing transaction.
   * If the container has not yet joined the transaction represented by the handle it is registered now.
   *
   * @param createIfAbsent A flag indicating if a new transaction should be started if no transaction is active
   * @return a handle (type {@link JacisTransactionHandle}) for the transaction currently associated with the current thread.
   * @throws JacisNoTransactionException If no transaction is active and the enforceTx flag is set to true
   */
  public JacisTransactionHandle getCurrentTransaction(boolean createIfAbsent) throws JacisNoTransactionException {
    JacisTransactionHandle handle = null;
    if (createIfAbsent) { // create a new TX if not yet present
      if (txAdapter.isTransactionActive()) { // JTA TX active -> create (if not yet created) and join
        handle = txAdapter.joinCurrentTransaction(this);
      } else { // no JTA active -> can not create and join
        throw new JacisNoTransactionException("No active transaction!");
      }
    } else { // createIfAbsent == false // only return TX if already
      if (txAdapter.isTransactionActive()) {
        handle = txAdapter.joinCurrentTransaction(this);
      }
    }
    JacisTransactionInfo lastFinishedTxInfo = lastFinishedTransactionInfo.get();
    if (lastFinishedTxInfo != null) {
      if (handle != null && !lastFinishedTxInfo.getTxId().equals(handle.getTxId())) {
        lastFinishedTransactionInfo.remove();
      }
    }
    return handle;
  }

  public List<JacisTransactionInfo> getTransactionInfos() {
    Collection<JacisTransactionHandle> handles = txAdapter.getAllTransactionHandles();
    List<JacisTransactionInfo> res = new ArrayList<>(handles.size());
    long snapshotTimeMs = System.currentTimeMillis();
    for (JacisTransactionHandle txHandle : handles) {
      res.add(new JacisTransactionInfo(txHandle, this, storeMap.values(), snapshotTimeMs));
    }
    return res;
  }

  public JacisTransactionInfo getLastFinishedTransactionInfo() {
    return lastFinishedTransactionInfo.get();
  }

  public JacisTransactionInfo getTransactionInfo(Object externalTransaction) {
    JacisTransactionHandle txHandle = txAdapter.getTransactionHandle(externalTransaction);
    return getTransactionInfo(txHandle);
  }

  public JacisTransactionInfo getTransactionInfo(JacisTransactionHandle txHandle) {
    return txHandle == null ? null : new JacisTransactionInfo(txHandle, this, storeMap.values(), System.currentTimeMillis());
  }

  /**
   * Create a read only view of the current transaction context for all stores in this container.
   * This view can be used (read only) in a different thread.
   * This can be used to share one single transaction view in several threads.
   * Before accessing the object store the other thread should set the returned context
   * with the method {@link #startReadOnlyTransactionWithContext(JacisContainerReadOnlyTransactionContext)}.
   *
   * @param withTxName transaction name used for the read only view.
   * @return a read only view of the current transaction context of all stores.
   */
  public JacisContainerReadOnlyTransactionContext createReadOnlyTransactionView(String withTxName) {
    JacisContainerReadOnlyTransactionContext ctx = new JacisContainerReadOnlyTransactionContext();
    for (JacisStore<?, ?> store : storeMap.values()) {
      ctx.add(store, store.createReadOnlyTransactionView(withTxName));
    }
    return ctx;
  }

  /**
   * Starts a new (read only) transaction with the passed transaction context for all stores in this container.
   * The new transaction will work on a read only snapshot of the original transaction (where the context is obtained from).
   *
   * @param readOnlyTxContext the transaction context of the original transaction.
   */
  public void startReadOnlyTransactionWithContext(JacisContainerReadOnlyTransactionContext readOnlyTxContext) {
    readOnlyTxContext.startReadOnlyTransactionWithContext();
  }

  protected boolean hasAnyUpdatesPendingForTx() {
    for (JacisStore<?, ?> store : storeMap.values()) {
      if (((JacisStoreImpl<?, ?, ?>) store).hasObjectsUpdatedInCurrentTxView()) {
        return true;
      }
    }
    return false;
  }

  protected boolean hasStoreWithPendingDirtyCheck() {
    for (JacisStore<?, ?> store : storeMap.values()) {
      if (store.getObjectTypeSpec().getDirtyCheck() != null) {
        JacisStoreImpl<?, ?, ?> storeImpl = (JacisStoreImpl<?, ?, ?>) store;
        if (!storeImpl.isInReadOnlyTransaction() && !storeImpl.isCommitPending()) { // read only or already prepared TXs will not do (another) dirty check
          return true;
        }
      }
    }
    return false;
  }

  protected boolean hasAnyTransactionListenersNeedingSynchronousExecutionForPrepare() {
    for (JacisTransactionListener txListener : txListeners) {
      if (txListener.isSynchronizedExcecutionRequiredForPrepare()) {
        return true;
      }
    }
    return false;
  }

  protected boolean hasAnyTransactionListenersNeedingSynchronousExecutionForCommit() {
    for (JacisTransactionListener txListener : txListeners) {
      if (txListener.isSynchronizedExcecutionRequiredForCommit()) {
        return true;
      }
    }
    return false;
  }

  protected boolean hasAnyTransactionListenersNeedingSynchronousExecutionForRollback() {
    for (JacisTransactionListener txListener : txListeners) {
      if (txListener.isSynchronizedExcecutionRequiredForRollback()) {
        return true;
      }
    }
    return false;
  }


  /**
   * Prepare the transaction represented by the passed transaction handle.
   * Note that this method usually is only called internally.
   * Usually the internalCommit is called either at the local transaction returned
   * by the method starting this transaction ({@link #beginLocalTransaction(String)}),
   * or by the external transaction framework via the transaction adapter.
   *
   * @param transaction The transaction handle representing the transaction to prepare.
   */
  public void internalPrepare(JacisTransactionHandle transaction) {
    boolean executeSynchronized = hasAnyUpdatesPendingForTx() // if any store has updated entries we need to synchronize
        || hasStoreWithPendingDirtyCheck() // if any store has a dirty check pending (may causing updated entries) we need to synchronize
        || hasAnyTransactionListenersNeedingSynchronousExecutionForPrepare(); // if any transaction listener requires sync. execution we need to synchronize
    if (executeSynchronized) {
      transactionDemarcationLock.writeLock().lock();
    }
    try {
      txListeners.forEach(l -> l.beforePrepare(this, transaction));
      for (JacisStore<?, ?> store : storeMap.values()) {
        ((JacisStoreTransactionAdapter) store).internalPrepare(transaction);
      }
      txListeners.forEach(l -> l.afterPrepare(this, transaction));
    } finally {
      if (executeSynchronized) {
        transactionDemarcationLock.writeLock().unlock();
      }
    }
  }

  // ======================================================================================
  // synchronized execution
  // ======================================================================================

  private Supplier<Object> runnableWrapper(Runnable r) {
    return () -> {
      r.run();
      return null;
    };
  }

  public ReadWriteLock getTransactionDemarcationLock() {
    return transactionDemarcationLock;
  }

  private <R> R withReadLock(Supplier<R> task) {
    transactionDemarcationLock.readLock().lock(); // <======= **READ** LOCK =====
    try {
      return task.get();
    } finally {
      transactionDemarcationLock.readLock().unlock(); // <======= **READ** UNLOCK =====
    }
  }

  /**
   * Execute the passed operation (without return value) as a global atomic operation (atomic over all stores).
   * The execution of global atomic operations can not overlap with the execution of a commit (changing the visible data) of another transaction (but normal operations on other transactions may overlap),
   * even if the commit is (currently) executed for any other store belonging to the same JACIS container.
   *
   * @param atomicOperation The operation to execute atomically
   */
  public void executeGlobalAtomic(Runnable atomicOperation) { // Execute a global atomic operation. No prepare / commit / rollback of any other TX and no other global atomic action for any store will interleave.
    withReadLock(runnableWrapper(atomicOperation));
  }

  /**
   * Execute the passed operation (with return value) as a global atomic operation (atomic over all stores).
   * The execution of global atomic operations can not overlap with the execution of a commit (changing the visible data) of another transaction (but normal operations on other transactions may overlap),
   * even if the commit is (currently) executed for any other store belonging to the same JACIS container.
   *
   * @param atomicOperation The operation to execute atomically
   * @param <R>             The return type of the operation
   * @return The return value of the operation
   */
  public <R> R computeGlobalAtomic(Supplier<R> atomicOperation) { // Execute a global atomic operation for the current store. No prepare / commit / rollback of any other TX and no other global atomic action for any store will interleave.
    return withReadLock(atomicOperation);
  }

  /**
   * Commit the transaction represented by the passed transaction handle.
   * Note that this method usually is only called internally.
   * Usually the internalCommit is called either at the local transaction returned
   * by the method starting this transaction ({@link #beginLocalTransaction(String)}),
   * or by the external transaction framework via the transaction adapter.
   *
   * @param transaction The transaction handle representing the transaction to internalCommit.
   */
  public void internalCommit(JacisTransactionHandle transaction) { // NO-API
    boolean executeSynchronized = hasAnyUpdatesPendingForTx() // if any store has updated entries we need to synchronize
        || hasStoreWithPendingDirtyCheck() // if any store has a dirty check pending (may causing updated entries) we need to synchronize
        || hasAnyTransactionListenersNeedingSynchronousExecutionForCommit(); // if any transaction listener requires sync. execution we need to synchronize
    if (executeSynchronized) {
      transactionDemarcationLock.writeLock().lock();
    }
    try {
      txListeners.forEach(l -> l.beforeCommit(this, transaction));
      List<AbstractMap.SimpleImmutableEntry<JacisStore<?, ?>, Throwable>> exceptions = null;
      for (JacisStore<?, ?> store : storeMap.values()) {
        try {
          ((JacisStoreTransactionAdapter) store).internalCommit(transaction);
        } catch (Throwable e) {
          exceptions = exceptions != null ? exceptions : new ArrayList<>();
          exceptions.add(new SimpleImmutableEntry<>(store, e));
        }
      }
      JacisTransactionInfo txInfo = getTransactionInfo(transaction);
      if (txInfo != null) {
        lastFinishedTransactionInfo.set(txInfo);
      }
      for (JacisStore<?, ?> store : storeMap.values()) {
        try {
          ((JacisStoreTransactionAdapter) store).internalDestroy(transaction);
        } catch (Throwable e) {
          exceptions = exceptions != null ? exceptions : new ArrayList<>();
          exceptions.add(new SimpleImmutableEntry<>(store, new JacisInternalException("Exception during internalDestroy after commit for TX " + transaction.getTxId() + " on store " + store.getStoreIdentifier().toShortString())));
        }
      }
      txListeners.forEach(l -> l.afterCommit(this, transaction));
      txAdapter.disjoinCurrentTransaction(transaction);
      if (exceptions != null && !exceptions.isEmpty()) {
        buildAndThrowException(transaction, true, exceptions);
      }
    } finally {
      if (executeSynchronized) {
        transactionDemarcationLock.writeLock().unlock();
      }
      transaction.getJfrEvent().trackCommit(storeMap.size()).commit();
    }
  }

  /**
   * Rollback the transaction represented by the passed transaction handle.
   * Note that this method usually is only called internally.
   * Usually the internalCommit is called either at the local transaction returned
   * by the method starting this transaction ({@link #beginLocalTransaction(String)}),
   * or by the external transaction framework via the transaction adapter.
   *
   * @param transaction The transaction handle representing the transaction to rollback.
   */
  public void internalRollback(JacisTransactionHandle transaction) { // NO-API
    boolean executeSynchronized = hasAnyUpdatesPendingForTx() // if any store has updated entries we need to synchronize (dirty check can be ignored here)
        || hasAnyTransactionListenersNeedingSynchronousExecutionForRollback(); // if any transaction listener requires sync. execution we need to synchronize
    if (executeSynchronized) {
      transactionDemarcationLock.writeLock().lock();
    }
    try {
      txListeners.forEach(l -> l.beforeRollback(this, transaction));
      for (JacisStore<?, ?> store : storeMap.values()) {
        ((JacisStoreTransactionAdapter) store).internalRollback(transaction);
      }
      JacisTransactionInfo txInfo = getTransactionInfo(transaction);
      if (txInfo != null) {
        lastFinishedTransactionInfo.set(txInfo);
      }
      List<AbstractMap.SimpleImmutableEntry<JacisStore<?, ?>, Throwable>> exceptions = null;
      for (JacisStore<?, ?> store : storeMap.values()) {
        try {
          ((JacisStoreTransactionAdapter) store).internalDestroy(transaction);
        } catch (Throwable e) {
          exceptions = exceptions != null ? exceptions : new ArrayList<>();
          exceptions.add(new SimpleImmutableEntry<>(store, new JacisInternalException("Exception during internalDestroy during rollback for TX " + transaction.getTxId() + " on store " + store.getStoreIdentifier().toShortString())));
        }
      }
      txListeners.forEach(l -> l.afterRollback(this, transaction));
      txAdapter.disjoinCurrentTransaction(transaction);
      if (exceptions != null && !exceptions.isEmpty()) {
        buildAndThrowException(transaction, false, exceptions);
      }
    } finally {
      if (executeSynchronized) {
        transactionDemarcationLock.writeLock().unlock();
      }
      transaction.getJfrEvent().trackRollback(storeMap.size()).commit();
    }
  }

  protected void buildAndThrowException(JacisTransactionHandle transaction, boolean commit, List<AbstractMap.SimpleImmutableEntry<JacisStore<?, ?>, Throwable>> exceptions) {
    Throwable firstException = exceptions.get(0).getValue(); // use the first exception as master
    RuntimeException masterException;
    if (firstException instanceof RuntimeException) {
      masterException = (RuntimeException) firstException;
      if (commit) {
        masterException.addSuppressed(new JacisTxCommitException("Committing TX " + transaction.getTxId() + " caused " + exceptions.size() + " exceptions (committing " + storeMap.size() + " stores)."));
      } else {
        masterException.addSuppressed(new JacisTxRollbackException("Rollback TX " + transaction.getTxId() + " caused " + exceptions.size() + " exceptions (rolling " + storeMap.size() + " stores back)"));
      }
    } else if (commit) {
      masterException = new JacisTxCommitException("Committing TX " + transaction.getTxId() + " caused " + exceptions.size() + " exceptions (committing " + storeMap.size() + " stores).", firstException);
    } else {
      masterException = new JacisTxRollbackException("Rollback TX " + transaction.getTxId() + " caused " + exceptions.size() + " exceptions (rolling" + storeMap.size() + " stores back).", firstException);
    }
    for (int i = 1; i < exceptions.size(); i++) { // skipping the first since it is the master
      masterException.addSuppressed(exceptions.get(i).getValue());
    }
    throw masterException;
  }

  /**
   * Store identifier uniquely identifying a store inside a container.
   * The store is identified by the type of the keys and the type of the values in the store.
   */
  @JacisApi
  public static class StoreIdentifier {

    /** The namespace where ths store lives. In one namespace there may only be one store for a key and value class combination. */
    private final String storeNamespace;
    /** Type of the keys in the store */
    private final Class<?> keyClass;
    /** Type of the values in the store */
    private final Class<?> valueClass;

    /**
     * Create a store identifier with the passed types for the keys and values.
     *
     * @param storeNamespace   Namespace of the store
     * @param keyClass         Type of the keys in the store
     * @param valueClass       Type of the values in the store
     */
    StoreIdentifier(String storeNamespace, Class<?> keyClass, Class<?> valueClass) {
      this.storeNamespace = storeNamespace;
      this.keyClass = keyClass;
      this.valueClass = valueClass;
    }


    /** @return The namespace of the store. */
    public String storeNamespace() {
        return storeNamespace;
    }

    /** @return The type of the keys in the store. */
    public Class<?> getKeyClass() {
      return keyClass;
    }

    /** @return The type of the values in the store. */
    public Class<?> getValueClass() {
      return valueClass;
    }

    public String toShortString() {
      return storeNamespace +"|"+ keyClass.getSimpleName() + "->" + valueClass.getSimpleName();
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "(namespace= "+storeNamespace+", keyClass=" + keyClass + ", valueClass=" + valueClass + ")";
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((storeNamespace == null) ? 0 : storeNamespace.hashCode());
      result = prime * result + ((keyClass == null) ? 0 : keyClass.hashCode());
      result = prime * result + ((valueClass == null) ? 0 : valueClass.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      } else if (obj == null) {
        return false;
      } else if (getClass() != obj.getClass()) {
        return false;
      }
      StoreIdentifier that = (StoreIdentifier) obj;
      return Objects.equals(this.storeNamespace, that.storeNamespace)
         && Objects.equals(this.keyClass, that.keyClass) //
         && Objects.equals(this.valueClass, that.valueClass);
    }

  } // END OF: public static class StoreIdentifier {

  public static abstract class JacisStoreTransactionAdapter { // NO-API

    protected abstract void internalPrepare(JacisTransactionHandle transaction);

    protected abstract void internalCommit(JacisTransactionHandle transaction);

    protected abstract void internalRollback(JacisTransactionHandle transaction);

    protected abstract void internalDestroy(JacisTransactionHandle transaction);

  } // END OF: public static abstract class JacisStoreTransactionAdapter {

}
