/**
 * Copyright (c) 2010 Jan Wiemer
 */
package org.jacis.container;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import org.jacis.exception.JacisNoTransactionException;
import org.jacis.exception.JacisStaleObjectException;
import org.jacis.plugin.JacisTransactionListener;
import org.jacis.plugin.txadapter.JacisTransactionAdapter;
import org.jacis.plugin.txadapter.local.JacisLocalTransaction;
import org.jacis.plugin.txadapter.local.JacisTransactionAdapterLocal;
import org.jacis.store.JacisStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Jan Wiemer
 *
 * The main class of the Java ACI Store.
 * It provides methods to create stores for different object types and provides access to those stores.
 */
public class JacisContainer {

  private static final Logger log = LoggerFactory.getLogger(JacisContainer.class);

  protected final JacisTransactionAdapter txAdapter;
  protected final Map<StoreIdentifier, JacisStore<?, ?, ?>> storeMap = new ConcurrentHashMap<>();
  protected final List<JacisTransactionListener> txListeners = new ArrayList<>();

  public JacisContainer(JacisTransactionAdapter txAdapter) {
    this.txAdapter = txAdapter;
  }

  public JacisContainer() {
    this(new JacisTransactionAdapterLocal());
  }

  public JacisContainer registerTransactionListener(JacisTransactionListener listener) {
    txListeners.add(listener);
    return this;
  }

  public <K, TV, CV> JacisStore<K, TV, CV> createStore(JacisObjectTypeSpec<K, TV, CV> objectTypeSpec) {
    StoreIdentifier storeIdentifier = new StoreIdentifier(objectTypeSpec.getKeyClass(), objectTypeSpec.getValueClass());
    JacisStore<K, TV, CV> store = new JacisStore<K, TV, CV>(this, storeIdentifier, objectTypeSpec);
    storeMap.put(storeIdentifier, store);
    return store;
  }

  @SuppressWarnings("unchecked")
  public <K, TV, CV> JacisStore<K, TV, CV> getStore(Class<K> keyClass, Class<TV> valueClass) {
    StoreIdentifier storeIdentifier = new StoreIdentifier(keyClass, valueClass);
    return (JacisStore<K, TV, CV>) storeMap.get(storeIdentifier);
  }

  public synchronized void clearAllStores() {
    storeMap.values().forEach(store -> store.clear());
  }

  public boolean isInTransaction() {
    return txAdapter.getCurrentTransaction(false) != null;
  }

  public JacisLocalTransaction beginLocalTransaction() {
    String description = Stream.of(new Exception("-").getStackTrace())//
        .filter(se -> !getClass().getName().equals(se.getClassName()))//
        .map(se -> se.toString()).findFirst().orElse("-");
    return beginLocalTransaction(description);
  }

  public JacisLocalTransaction beginLocalTransaction(String description) {
    if (txAdapter instanceof JacisTransactionAdapterLocal) {
      JacisTransactionAdapterLocal txAdapterLocal = (JacisTransactionAdapterLocal) txAdapter;
      return txAdapterLocal.startLocalTransaction(this, description);
    } else {
      throw new IllegalStateException("Local transactions not supported! Local transactions need TX adapter " + JacisTransactionAdapterLocal.class.getSimpleName() + " but the configured is: " + txAdapter.getClass().getSimpleName());
    }
  }

  public void withLocalTxAndRetry(int retries, Runnable task) {
    while (retries-- > 0) {
      try {
        withLocalTx(task);
        return;
      } catch (JacisStaleObjectException e) {
        log.warn("Stale object exception caught: {}", "" + e);
        log.info("Detail message: \n{}", e.getDetails());
        if (retries == 0) {
          throw e;
        }
      }
    }
  }

  public void withLocalTx(Runnable task) {
    JacisLocalTransaction tx = beginLocalTransaction();
    Throwable txException = null;
    try {
      task.run();
      tx.prepare();
      tx.commit();
      tx = null;
    } catch (Throwable e) {
      txException = e;
      throw e;
    } finally {
      if (tx != null) {
        try {
          tx.rollback();
        } catch (Throwable rollbackException) {
          RuntimeException exceptionToThrow = new RuntimeException("Rollback failed after " + txException, txException);
          exceptionToThrow.addSuppressed(rollbackException);
          throw exceptionToThrow;
        }
      }
    }
  }

  public JacisTransactionHandle getCurrentTransaction(boolean enforceTx) throws JacisNoTransactionException {
    JacisTransactionHandle tx = txAdapter.getCurrentTransaction(enforceTx);
    if (tx != null) {
      txAdapter.joinCurrentTransaction(tx, this);
    }
    return tx;
  }

  public synchronized void prepare(JacisTransactionHandle transaction) {
    txListeners.stream().forEach(l -> l.beforePrepare(this, transaction));
    for (JacisStore<?, ?, ?> store : storeMap.values()) {
      store.prepare(transaction);
    }
    txListeners.stream().forEach(l -> l.afterPrepare(this, transaction));
  }

  public synchronized void commit(JacisTransactionHandle transaction) {
    txListeners.stream().forEach(l -> l.beforeCommit(this, transaction));
    for (JacisStore<?, ?, ?> store : storeMap.values()) {
      store.commit(transaction);
    }
    txListeners.stream().forEach(l -> l.afterCommit(this, transaction));
    txAdapter.destroyCurrentTransaction();
  }

  public synchronized void rollback(JacisTransactionHandle transaction) {
    txListeners.stream().forEach(l -> l.beforeRollback(this, transaction));
    for (JacisStore<?, ?, ?> store : storeMap.values()) {
      store.rollback(transaction);
    }
    txListeners.stream().forEach(l -> l.afterRollback(this, transaction));
    txAdapter.destroyCurrentTransaction();
  }

  public static class StoreIdentifier {
    private final Class<?> keyClass;
    private final Class<?> valueClass;

    public StoreIdentifier(Class<?> keyClass, Class<?> valueClass) {
      this.keyClass = keyClass;
      this.valueClass = valueClass;
    }

    public Class<?> getKeyClass() {
      return keyClass;
    }

    public Class<?> getValueClass() {
      return valueClass;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
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
      if (this.keyClass == null ? that.keyClass == null : this.keyClass.equals(that.keyClass)) {
        if (this.valueClass == null ? that.valueClass == null : this.valueClass.equals(that.valueClass)) {
          return true;
        }
      }
      return false;
    }

  }
}
