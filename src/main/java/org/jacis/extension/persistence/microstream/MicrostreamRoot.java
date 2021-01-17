package org.jacis.extension.persistence.microstream;

import java.util.HashMap;
import java.util.Map;

import org.jacis.container.JacisContainer.StoreIdentifier;
import org.jacis.store.JacisStore;

/**
 * The root object stored by the Microstream storage manager.
 * Basically it stores the list of the roots for the Jacis stores.
 * 
 * @author Jan Wiemer
 */
class MicrostreamRoot {

  /** Map containing the root object for each JACIS store. */
  private Map<StoreIdentifier, MicrostreamStoreRoot<?, ?>> storeRootMap = new HashMap<>();

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(" + storeRootMap.values() + ")";
  }

  Map<StoreIdentifier, MicrostreamStoreRoot<?, ?>> getStoreRootMap() {
    return storeRootMap;
  }

  @SuppressWarnings("unchecked")
  public <K, V> MicrostreamStoreRoot<K, V> getStoreRoot(JacisStore<K, V> store) {
    StoreIdentifier storeIdentifier = store.getStoreIdentifier();
    return (MicrostreamStoreRoot<K, V>) storeRootMap.get(storeIdentifier);
  }

  public <K, V> void setStoreRoot(JacisStore<K, V> store, MicrostreamStoreRoot<K, V> root) {
    StoreIdentifier storeIdentifier = store.getStoreIdentifier();
    storeRootMap.put(storeIdentifier, root);
  }

}