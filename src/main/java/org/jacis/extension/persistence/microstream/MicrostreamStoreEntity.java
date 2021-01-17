package org.jacis.extension.persistence.microstream;

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