/*
 * Copyright (c) 2020. Jan Wiemer
 */
package org.jacis.extension.persistence.microstream;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jacis.container.JacisContainer;
import org.jacis.container.JacisContainer.StoreIdentifier;
import org.jacis.container.JacisTransactionHandle;
import org.jacis.plugin.persistence.JacisPersistenceAdapter;
import org.jacis.store.JacisStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the JACIS persistence adapter based on Microstream serialization
 * (see <a href="https://microstream.one/">https://microstream.one/</a>).
 * 
 * Changes objects are passed to the Microstream storage manager on commit.
 * This way the data stored persistently is kept up to date.
 * If there is already persisted data on startup the JACIS store is initialized with this data.
 * 
 * Implementation hint:
 * For performance reasons the actual entities stored by Microstream are held
 * in a custom implementation of a double-linked list.
 * Each list entry represents a store entry and contains the key and the value of the object.
 * We refer this objest as the Microstream entity object for the store entry.
 * For each key a (transient) map stores a reference to the list entry object.
 * This way modifying, adding and removing objects can be done in constant time independently of the store size.
 * 
 * @param <K> Key type of the store entry
 * @param <V> Value type of the store entry
 * 
 * @author Jan Wiemer
 */
public class MicrostreamPersistenceAdapter<K, V> implements JacisPersistenceAdapter<K, V> {

  private static final Logger log = LoggerFactory.getLogger(MicrostreamPersistenceAdapter.class);

  /** Identifier of the JACIS store. */
  private StoreIdentifier storeIdentifier;
  /** Flag that can be used to enable trace logging in the adapter implementation. */
  private boolean traceLogging;
  /** The Microstream multistore persistence adapter used to persist entities. */
  private MicrostreamStorage multistoreAdapter;
  /** The root object stored by the Microstream storage manager. */
  private MicrostreamStoreRoot<K, V> storageRoot;
  /** A map storing the Microstream entity objects stored by the Microstream storage manager for each key in the store. */
  private Map<K, MicrostreamStoreEntity<K, V>> key2entity;
  /** Set of Microstream entity objects modified during the transaction. */
  private Set<Object> objectsToStore = null;
  /** Number of threads used to initialize the JACIS store with the initial (already stored) elements */
  private int initStoreThreads = 4;

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

  public MicrostreamPersistenceAdapter(MicrostreamStorage multistoreAdapter, boolean traceLogging) {
    this.multistoreAdapter = multistoreAdapter;
    this.traceLogging = traceLogging;
    key2entity = new HashMap<>();
  }

  public MicrostreamPersistenceAdapter(MicrostreamStorage multistoreAdapter) {
    this(multistoreAdapter, false);
  }

  public MicrostreamPersistenceAdapter<K, V> setInitStoreThreads(int initStoreThreads) {
    this.initStoreThreads = initStoreThreads;
    return this;
  }

  @Override
  public void initializeStore(JacisStore<K, V> store) {
    storeIdentifier = store.getStoreIdentifier();
    log.debug("{} start store initialization for store {}...", this, storeIdentifier.toShortString());
    long t0 = System.nanoTime();
    storageRoot = multistoreAdapter.getOrCreateStoreRoot(store);
    List<MicrostreamStoreEntity<K, V>> rootList = storageRoot.toList();
    store.initStoreNonTransactional(rootList, e -> e.getKey(), e -> e.getValue(), initStoreThreads);
    log.debug("{} init finished after {} (store: {}, storage root: {} (found {} persisted entries), initial store size: {})", //
        this, stopTime(t0), storeIdentifier.toShortString(), storageRoot, rootList.size(), store.size());
    if (traceLogging) {
      log.trace("{} init: -> storage root list: {}", this, rootList);
    }
  }

  @Override
  public void onModification(K key, V oldValue, V newValue, JacisTransactionHandle tx) {
    MicrostreamStoreEntity<K, V> entity = key2entity.get(key);
    objectsToStore = objectsToStore != null ? objectsToStore : new HashSet<>();
    if (entity != null && newValue != null) {
      entity.setValue(newValue);
      objectsToStore.add(entity);
    } else if (entity != null && newValue == null) {
      storageRoot.remove(entity, objectsToStore);
      key2entity.remove(key);
    } else if (entity == null && newValue != null) {
      MicrostreamStoreEntity<K, V> newEntity = new MicrostreamStoreEntity<>(key, newValue);
      storageRoot.add(newEntity, objectsToStore);
      key2entity.put(key, newEntity);
    } else { // (entity == null && newValue == null)
      // nothing to do...
    }
    if (traceLogging) {
      log.trace("{} track modification for {} (store: {}, old vale: {}, new value: {}) (TX: {})", this, key, storeIdentifier.toShortString(), oldValue, newValue, tx);
    }
  }

  @Override
  public void afterCommitForStore(JacisStore<K, V> store, JacisTransactionHandle tx) {
    log.debug("{} after commit for store track #{} objects to store. (store: {}, TX: {})", //
        this, objectsToStore == null ? 0 : objectsToStore.size(), storeIdentifier.toShortString(), tx);
    if (objectsToStore != null && !objectsToStore.isEmpty()) {
      multistoreAdapter.trackObjectsToStore(tx, objectsToStore);
    }
    if (traceLogging) {
      log.trace("{} after commit for store -> tracked objects to store: {}", this, objectsToStore);
      log.trace("{} after commit for store -> persistent list: {}", this, storageRoot.toList());
    }
    objectsToStore = null;
  }

  @Override
  public void afterRollbackForStore(JacisStore<K, V> store, JacisTransactionHandle tx) {
    log.debug("{} after rollback for store skip #{} objects to store. (store: {}, TX: {})", //
        this, objectsToStore == null ? 0 : objectsToStore.size(), storeIdentifier.toShortString(), tx);
    if (traceLogging) {
      log.trace("{} after rollback for store -> skipped objects to store: {}", this, objectsToStore);
      log.trace("{} after rollback for store -> persistent list: {}", this, storageRoot.toList());
    }
    objectsToStore = null;
  }

  @Override
  public void afterCommit(JacisContainer container, JacisTransactionHandle tx) {
    multistoreAdapter.afterCommit(tx);
  }

  @Override
  public void afterRollback(JacisContainer container, JacisTransactionHandle tx) {
    multistoreAdapter.afterRollback(tx);
  }

  private String stopTime(long t0) {
    return multistoreAdapter.stopTime(t0);
  }

}
