/*
 * Copyright (c) 2020. Jan Wiemer
 */
package org.jacis.extension.persistence;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jacis.container.JacisTransactionHandle;
import org.jacis.plugin.persistence.JacisPersistenceAdapter;
import org.jacis.store.JacisStoreImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import one.microstream.storage.types.StorageManager;

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

  /** Flag that can be used to enable trace logging in the adapter implementation. */
  private boolean traceLogging;
  /** The Microstream storage manager used to persist entities. */
  private StorageManager storageManager;
  /** The root object stored by the Microstream storage manager. */
  private MicrostreamStoreRoot<K, V> storageRoot;
  /** A map storing the Microstream entity objects stored by the Microstream storage manager for each key in the store. */
  private Map<K, MicrostreamStoreEntity<K, V>> key2entity;
  /** Set of Microstream entity objects modified during the transaction. */
  private Set<Object> objectsToStore = null;

  @Override
  public String toString() {
    return "MPA(" + (storageRoot == null ? "-" : storageRoot.getStoreName()) + ")";
  }

  public MicrostreamPersistenceAdapter(StorageManager storageManager, boolean traceLogging) {
    this.storageManager = storageManager;
    this.traceLogging = traceLogging;
    key2entity = new HashMap<>();
    log.debug("{} instantiated (storageManager: {})", this, storageManager);
    log.debug("{}  - storage directory {}", this, storageManager.configuration().fileProvider().baseDirectory().toPathString());
  }

  public MicrostreamPersistenceAdapter(StorageManager storageManager) {
    this(storageManager, false);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void initializeStore(JacisStoreImpl<K, V, ?> store) {
    log.debug("{} start initialization...", this);
    long t0 = System.nanoTime();
    storageManager.start();
    if (storageManager.root() == null) {
      storageRoot = new MicrostreamStoreRoot<>(store.getStore().getStoreIdentifier().toShortString());
      storageManager.setRoot(storageRoot);
      storageManager.storeRoot();
      log.debug("{} initialization: created new root; {}", this, storageRoot);
    } else {
      storageRoot = (MicrostreamStoreRoot<K, V>) storageManager.root();
      log.debug("{} initialization: found existing root; {}", this, storageRoot);
    }
    List<MicrostreamStoreEntity<K, V>> rootList = storageRoot.toList();
    store.initStoreNonTransactional(rootList, e -> e.getKey(), e -> e.getValue(), 4);
    log.debug("{} initialized after {} (storage root: {} (size: {}), initial size: {})", this, stopTime(t0), storageRoot, rootList.size(), store.size());
    if (traceLogging) {
      log.trace("{} initialized -> persistent list: {}", this, storageRoot.toList());
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
      log.trace("{} track modification for {} ", this, key);
    }
  }

  @Override
  public void prepareCommit() {
    log.debug("{} prepareCommit", this);
    // nothing to do here, we expect the microstream storage to be always available
  }

  @Override
  public void commit() {
    log.debug("{} commit (#{} objects to store: {}", this, objectsToStore == null ? 0 : objectsToStore.size(), objectsToStore);
    long t0 = System.nanoTime();
    if (objectsToStore != null && !objectsToStore.isEmpty()) {
      storageManager.storeAll(objectsToStore);
    }
    log.debug("{} commit took {}", this, stopTime(t0));
    if (traceLogging) {
      log.trace("{} commit -> persistent list: {}", this, storageRoot.toList());
    }
    objectsToStore = null;
  }

  @Override
  public void rollback() {
    log.debug("{} rollback (#{} objects to store: {}", this, objectsToStore == null ? 0 : objectsToStore.size(), objectsToStore);
    objectsToStore = null; // discard changes
  }

  protected String stopTime(long startTimeNs) {
    long nanos = System.nanoTime() - startTimeNs;
    double millis = ((double) nanos) / (1000 * 1000);
    return String.format("%.2f ms", millis);
  }

}

//====================================================================================
//====================================================================================
//====================================================================================
//====================================================================================

/**
 * The root object stored by the Microstream storage manager.
 * Basically it stores (the head of) the linked list of Microstream entity objects representing the store entries.
 * 
 * @author Jan Wiemer
 *
 * @param <K> Key type of the store entry
 * @param <V> Value type of the store entry
 */
class MicrostreamStoreRoot<K, V> {

  /** The name of the JACIS store represented by this root object. */
  private final String storeName;
  /** The first object of the linked list of Microstream entity objects representing the store entries. */
  private MicrostreamStoreEntity<K, V> firstElement;

  public MicrostreamStoreRoot(String storeName) {
    this.storeName = storeName;
  }

  @Override
  public String toString() {
    return "StorageRoot(" + storeName + ")";
  }

  public void add(MicrostreamStoreEntity<K, V> entity, Set<Object> objectsToStore) {
    if (firstElement != null) {
      firstElement.setPrev(entity);
      entity.setNext(firstElement);
    }
    firstElement = entity;
    objectsToStore.add(entity);
    objectsToStore.add(this);
  }

  public void remove(MicrostreamStoreEntity<K, V> entity, Set<Object> objectsToStore) {
    MicrostreamStoreEntity<K, V> prev = entity.getPrev();
    MicrostreamStoreEntity<K, V> next = entity.getNext();
    if (prev != null) {
      prev.setNext(next);
      objectsToStore.add(prev);
    } else {
      objectsToStore.add(this);
      firstElement = next;
    }
    if (next != null) {
      next.setPrev(prev);
      objectsToStore.add(next);
    }
    entity.setPrev(null).setNext(null);
    objectsToStore.add(entity);
  }

  public MicrostreamStoreEntity<K, V> getFirstElement() {
    return firstElement;
  }

  public void setFirstElement(MicrostreamStoreEntity<K, V> firstElement) {
    this.firstElement = firstElement;
  }

  public String getStoreName() {
    return storeName;
  }

  public List<MicrostreamStoreEntity<K, V>> toList() {
    List<MicrostreamStoreEntity<K, V>> res = new ArrayList<>();
    MicrostreamStoreEntity<K, V> e = firstElement;
    while (e != null) {
      res.add(e);
      e = e.getNext();
    }
    return res;
  }

}

//====================================================================================
//====================================================================================
//====================================================================================
//====================================================================================

/**
 * The Microstream entity object representing the store entries.
 * The entity objects build a linked list (see {@link #getPrev()} and {@link #getNext()}).
 * 
 * @author Jan Wiemer
 *
 * @param <K> Key type of the store entry
 * @param <V> Value type of the store entry
 */
class MicrostreamStoreEntity<K, V> {

  /** The key of the store entry represented by this Microstream entity object. */
  private K key;
  /** The value of the store entry represented by this Microstream entity object. */
  private V value;
  /** The next element in the linked list. */
  private MicrostreamStoreEntity<K, V> next;
  /** The previous element in the linked list. */
  private MicrostreamStoreEntity<K, V> prev;

  public MicrostreamStoreEntity(K key, V value) {
    this.key = key;
    this.value = value;
  }

  public K getKey() {
    return key;
  }

  public V getValue() {
    return value;
  }

  public MicrostreamStoreEntity<K, V> getNext() {
    return next;
  }

  public MicrostreamStoreEntity<K, V> getPrev() {
    return prev;
  }

  public MicrostreamStoreEntity<K, V> setKey(K key) {
    this.key = key;
    return this;
  }

  public MicrostreamStoreEntity<K, V> setValue(V value) {
    this.value = value;
    return this;
  }

  public MicrostreamStoreEntity<K, V> setNext(MicrostreamStoreEntity<K, V> next) {
    this.next = next;
    return this;
  }

  public MicrostreamStoreEntity<K, V> setPrev(MicrostreamStoreEntity<K, V> prev) {
    this.prev = prev;
    return this;
  }

  @Override
  public String toString() {
    return "Entity(" + key + ")";
  }
}
