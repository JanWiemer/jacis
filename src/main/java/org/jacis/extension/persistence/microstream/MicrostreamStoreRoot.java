package org.jacis.extension.persistence.microstream;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.jacis.container.JacisContainer;
import org.jacis.container.JacisContainer.StoreIdentifier;

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

  /** The identifier of the JACIS store represented by this root object. */
  private final JacisContainer.StoreIdentifier storeIdentifier;
  /** The first object of the linked list of Microstream entity objects representing the store entries. */
  private MicrostreamStoreEntity<K, V> firstElement;

  public MicrostreamStoreRoot(JacisContainer.StoreIdentifier storeIdentifier) {
    this.storeIdentifier = storeIdentifier;
  }

  @Override
  public String toString() {
    StringBuilder b = new StringBuilder();
    b.append("StorageRoot(").append(storeIdentifier.toShortString());
    b.append(")");
    return b.toString();
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

  public StoreIdentifier getStoreIdentifier() {
    return storeIdentifier;
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