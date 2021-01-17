/*
 * Copyright (c) 2020. Jan Wiemer
 */
package org.jacis.extension.persistence.microstream;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.jacis.container.JacisContainer.StoreIdentifier;
import org.jacis.container.JacisTransactionHandle;
import org.jacis.store.JacisStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import one.microstream.storage.types.StorageManager;

/**
 * Representing a single microstream storage manager storing to one storage directory.
 * A single instance of this class can be set for multiple JACIS stores managed by the same JACIs container.
 * In this the store operation for all committed objects of all stores is done in an atomic call after the commit.
 * 
 * @author Jan Wiemer
 */
public class MicrostreamStorage {

  private static final Logger log = LoggerFactory.getLogger(MicrostreamStorage.class);

  /** The Microstream storage manager used to persist entities. */
  private StorageManager storageManager;
  /** The root object stored by the Microstream storage manager. */
  private MicrostreamRoot storageRoot;
  /** For each active transaction the set of modified (wrapper) objects to store */
  private Map<JacisTransactionHandle, Set<Object>> objectsToStore = new HashMap<>();

  public MicrostreamStorage(StorageManager storageManager) {
    this.storageManager = storageManager;
    String storageDirectory = storageManager.configuration().fileProvider().baseDirectory().toPathString();
    log.debug("{} start init (storage dir: {})", this, storageDirectory);
    long t0 = System.nanoTime();
    if (storageManager.root() == null) {
      storageRoot = new MicrostreamRoot();
      storageManager.setRoot(storageRoot);
      storageManager.storeRoot();
      log.debug("{} init: created new root; {}", this, storageRoot);
    } else {
      storageRoot = (MicrostreamRoot) storageManager.root();
      log.debug("{} init: found existing root; {}", this, storageRoot);
    }
    log.debug("{} init finished after {} (storage root: {})", this, stopTime(t0), storageRoot);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

  public <K, V> MicrostreamStoreRoot<K, V> getOrCreateStoreRoot(JacisStore<K, V> store) {
    StoreIdentifier storeIdentifier = store.getStoreIdentifier();
    MicrostreamStoreRoot<K, V> storeRoot = storageRoot.getStoreRoot(store);
    if (storeRoot != null) {
      log.debug("{} ... found existing store root for {}: {}", this, storeIdentifier.toShortString(), storeRoot);
    } else {
      storeRoot = new MicrostreamStoreRoot<>(storeIdentifier);
      storageRoot.setStoreRoot(store, storeRoot);
      storageManager.storeAll(storageRoot, storageRoot.getStoreRootMap(), storeRoot);
      log.debug("{} ... created new store root for {}: {}", this, storeIdentifier.toShortString(), storeRoot);
    }
    return storeRoot;
  }

  public void trackObjectsToStore(JacisTransactionHandle tx, Set<Object> toStore) {
    Set<Object> oldToStore = objectsToStore.get(tx);
    if (oldToStore == null) {
      objectsToStore.put(tx, toStore);
    } else {
      oldToStore.addAll(toStore);
    }
  }

  public void afterCommit(JacisTransactionHandle tx) {
    Set<Object> toStore = objectsToStore.remove(tx);
    if (log.isTraceEnabled()) {
      log.trace("{} after commit: store #{} objects: {}", this, toStore == null ? 0 : toStore.size(), toStore);
    }
    long t0 = System.nanoTime();
    if (toStore != null && !toStore.isEmpty()) {
      storageManager.storeAll(toStore);
      log.debug("{} after commit: storing {} objects took {} (TX: {})", this, toStore.size(), stopTime(t0), tx);
    }
  }

  public void afterRollback(JacisTransactionHandle tx) {
    Set<Object> toStore = objectsToStore.remove(tx);
    if (log.isTraceEnabled()) {
      log.trace("{} after rollback: skip storing #{} objects: {}", this, toStore == null ? 0 : toStore.size(), toStore);
    }
    log.debug("{} after rollback: skip storing #{} (TX: {})", this, toStore == null ? 0 : toStore.size(), tx);
  }

  String stopTime(long startTimeNs) {
    long nanos = System.nanoTime() - startTimeNs;
    double millis = ((double) nanos) / (1000 * 1000);
    return String.format("%.2f ms", millis);
  }

}
