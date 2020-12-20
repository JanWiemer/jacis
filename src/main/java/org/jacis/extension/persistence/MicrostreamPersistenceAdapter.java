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
 * @author Jan Wiemer
 */
public class MicrostreamPersistenceAdapter<K, V> implements JacisPersistenceAdapter<K, V> {

  private static final Logger log = LoggerFactory.getLogger(MicrostreamPersistenceAdapter.class);

  private boolean traceLogging;
  private StorageManager storageManager;
  private MicrostreamStoreRoot<K, V> storageRoot;
  private Map<K, MicrostreamStoreEntity<K, V>> key2entity;
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
    if(traceLogging) {
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
    if(traceLogging) {
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
    if(traceLogging) {
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

/**
 * @author Jan Wiemer
 *
 * @param <K>
 * @param <V>
 */
class MicrostreamStoreRoot<K, V> {

  private final String storeName;
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

/**
 * @author Jan Wiemer
 *
 * @param <K>
 * @param <V>
 */
class MicrostreamStoreEntity<K, V> {

  private K key;
  private V value;
  private MicrostreamStoreEntity<K, V> next;
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
