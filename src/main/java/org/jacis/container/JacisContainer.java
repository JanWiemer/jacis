/**
 * Copyright (c) 2010 Jan Wiemer
 */
package org.jacis.container;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.jacis.exception.JacisNoTransactionException;
import org.jacis.exception.JacisStaleObjectException;
import org.jacis.plugin.JacisTransactionListener;
import org.jacis.plugin.txadapter.JacisLocalTransaction;
import org.jacis.plugin.txadapter.JacisTransactionAdapter;
import org.jacis.plugin.txadapter.JacisTransactionAdapterLocal;
import org.jacis.store.JacisStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Jan Wiemer
 *
 * The main class of the >Java ACI Store<.
 * It provides methods to create stores for different object types and provides access to those stores.
 */
public class JacisContainer {

  private static final Logger log = LoggerFactory.getLogger(JacisContainer.class);

  protected final JacisTransactionAdapter txAdapter;
  protected final Map<StoreIdentifier, JacisStore<?, ?>> storeMap = new ConcurrentHashMap<>();
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

  public <K, V> JacisStore<K, V> registerStore(JacisObjectTypeSpec<K, V> objectTypeSpec) {
    StoreIdentifier storeIdentifier = new StoreIdentifier(objectTypeSpec.getKeyClass(), objectTypeSpec.getValueClass());
    JacisStore<K, V> store = new JacisStore<K, V>(this, storeIdentifier, objectTypeSpec);
    storeMap.put(storeIdentifier, store);
    return store;
  }

  @SuppressWarnings("unchecked")
  public <K, V> JacisStore<K, V> getStore(Class<K> keyClass, Class<V> valueClass) {
    StoreIdentifier storeIdentifier = new StoreIdentifier(keyClass, valueClass);
    return (JacisStore<K, V>) storeMap.get(storeIdentifier);
  }

  public JacisLocalTransaction beginLocalTransaction() {
    if (txAdapter instanceof JacisTransactionAdapterLocal) {
      JacisTransactionAdapterLocal txAdapterLocal = (JacisTransactionAdapterLocal) txAdapter;
      return txAdapterLocal.startLocalTransaction(this);
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
        if (retries == 0) {
          throw e;
        }
      }
    }
  }

  public void withLocalTx(Runnable task) {
    JacisLocalTransaction tx = beginLocalTransaction();
    try {
      task.run();
      tx.prepare();
      tx.commit();
      tx = null;
    } finally {
      if (tx != null) {
        tx.rollback();
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
    for (JacisStore<?, ?> store : storeMap.values()) {
      store.prepare(transaction);
    }
    txListeners.stream().forEach(l -> l.afterPrepare(this, transaction));
  }

  public synchronized void commit(JacisTransactionHandle transaction) {
    txListeners.stream().forEach(l -> l.beforeCommit(this, transaction));
    for (JacisStore<?, ?> store : storeMap.values()) {
      store.commit(transaction);
    }
    txListeners.stream().forEach(l -> l.afterCommit(this, transaction));
    txAdapter.destroyCurrentTransaction();
  }

  public synchronized void rollback(JacisTransactionHandle transaction) {
    txListeners.stream().forEach(l -> l.beforeRollback(this, transaction));
    for (JacisStore<?, ?> store : storeMap.values()) {
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
