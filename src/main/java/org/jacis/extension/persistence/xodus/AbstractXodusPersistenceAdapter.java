/*
 * Copyright (c) 2020. Jan Wiemer
 */
package org.jacis.extension.persistence.xodus;

import java.util.ArrayList;
import java.util.List;

import org.jacis.container.JacisContainer.StoreIdentifier;
import org.jacis.container.JacisTransactionHandle;
import org.jacis.plugin.persistence.JacisPersistenceAdapter;
import org.jacis.store.JacisStore;
import org.jacis.store.KeyValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.env.Cursor;
import jetbrains.exodus.env.Environment;
import jetbrains.exodus.env.Store;
import jetbrains.exodus.env.StoreConfig;
import jetbrains.exodus.env.Transaction;
import jetbrains.exodus.env.TransactionalComputable;

/**
 * Abstract implementation of the JACIS persistence adapter based on JetBrains Xodus
 * (see <a href="https://github.com/JetBrains/xodus">https://github.com/JetBrains/xodus</a>).
 * 
 * The Xodus store is a key value store.
 * Keys and values are represented as <code>ByteIterable</code> instances.
 * Therefore the keys and objects in the Jacis store have to be converted into this format
 * before they can be stored.
 * This implementation defines two abstract methods <code>encode</code> and <code>decode</code>
 * for this conversion and leaves the implementation up to the derived classes.
 * 
 * @param <K> Key type of the store entry
 * @param <V> Value type of the store entry
 * 
 * @author Jan Wiemer
 */
public abstract class AbstractXodusPersistenceAdapter<K, V> implements JacisPersistenceAdapter<K, V> {

  private static final Logger log = LoggerFactory.getLogger(AbstractXodusPersistenceAdapter.class);

  /** Identifier of the JACIS store. */
  protected StoreIdentifier storeIdentifier;
  /** Flag that can be used to enable trace logging in the adapter implementation. */
  protected boolean traceLogging;
  /** Number of threads used to initialize the JACIS store with the initial (already stored) elements */
  private int initStoreThreads = 4;
  /** Set of objects modified during the transaction. */
  private List<KeyValuePair<ByteIterable, ByteIterable>> objectsToStore = null;
  /** The Xodus Environment providing methods to create stores and other helper methods. */
  private Environment xodusEnv;
  /** The Xodus store actually writing the objects to disk. */
  private Store xodusStore;

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

  public AbstractXodusPersistenceAdapter(Environment env, boolean traceLogging) {
    this.xodusEnv = env;
    this.traceLogging = traceLogging;
  }

  public AbstractXodusPersistenceAdapter(Environment env) {
    this(env, false);
  }

  public AbstractXodusPersistenceAdapter<K, V> setInitStoreThreads(int initStoreThreads) {
    this.initStoreThreads = initStoreThreads;
    return this;
  }

  protected abstract KeyValuePair<ByteIterable, ByteIterable> encode(K key, V value);

  protected abstract KeyValuePair<K, V> decode(ByteIterable xodusKey, ByteIterable xodusValue);

  @Override
  public void initializeStore(JacisStore<K, V> store) {
    storeIdentifier = store.getStoreIdentifier();
    log.debug("{} start store initialization for store {}...", this, storeIdentifier.toShortString());
    long t0 = System.nanoTime();
    xodusStore = xodusEnv.computeInTransaction(new TransactionalComputable<Store>() {
      @Override
      public Store compute(Transaction txn) {
        return xodusEnv.openStore(storeIdentifier.toString(), StoreConfig.WITHOUT_DUPLICATES, txn);
      }
    });
    List<KeyValuePair<K, V>> rootList = new ArrayList<>();
    xodusEnv.executeInReadonlyTransaction(xodusTx -> {
      try (Cursor cursor = xodusStore.openCursor(xodusTx)) {
        while (cursor.getNext()) {
          ByteIterable xodusKey = cursor.getKey(); // current key
          ByteIterable xodusValue = cursor.getValue(); // current value
          rootList.add(decode(xodusKey, xodusValue));
        }
      }
    });
    store.initStoreNonTransactional(rootList, e -> e.getKey(), e -> e.getVal(), initStoreThreads);
    log.debug("{} init finished after {} (store: {}, xodusStore: {} (found {} persisted entries), initial store size: {})", //
        this, stopTime(t0), storeIdentifier.toShortString(), xodusStore, rootList.size(), store.size());
  }

  @Override
  public void afterPrepareForStore(JacisStore<K, V> store, JacisTransactionHandle tx) {
    objectsToStore = new ArrayList<>();
  }

  @Override
  public void onModification(K key, V oldValue, V newValue, JacisTransactionHandle tx) {
    objectsToStore.add(encode(key, newValue));
    if (traceLogging) {
      log.trace("{} track modification for {} (store: {}, old vale: {}, new value: {}) (TX: {})", this, key, storeIdentifier.toShortString(), oldValue, newValue, tx);
    }
  }

  @Override
  public void afterCommitForStore(JacisStore<K, V> store, JacisTransactionHandle tx) {
    log.debug("{} after commit for store track #{} objects to store. (store: {}, TX: {})", //
        this, objectsToStore == null ? 0 : objectsToStore.size(), storeIdentifier.toShortString(), tx);
    xodusEnv.executeInExclusiveTransaction(xodusTx -> {
      for (KeyValuePair<ByteIterable, ByteIterable> toStore : objectsToStore) {
        ByteIterable xodusKey = toStore.getKey();
        ByteIterable xodusValue = toStore.getVal();
        if (xodusValue != null) {
          xodusStore.put(xodusTx, xodusKey, xodusValue);
        } else {
          xodusStore.delete(xodusTx, xodusKey);
        }
      }
    });
    if (traceLogging) {
      log.trace("{} after commit for store -> tracked objects to store: {}", this, objectsToStore);
    }
    objectsToStore = null;
  }

  @Override
  public void afterRollbackForStore(JacisStore<K, V> store, JacisTransactionHandle tx) {
    log.debug("{} after rollback for store skip #{} objects to store. (store: {}, TX: {})", //
        this, objectsToStore == null ? 0 : objectsToStore.size(), storeIdentifier.toShortString(), tx);
    if (traceLogging) {
      log.trace("{} after rollback for store -> skipped objects to store: {}", this, objectsToStore);
    }
    objectsToStore = null;
  }

  private String stopTime(long startTimeNs) {
    long nanos = System.nanoTime() - startTimeNs;
    double millis = ((double) nanos) / (1000 * 1000);
    return String.format("%.2f ms", millis);
  }

}
