/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.plugin;

import org.jacis.container.JacisTransactionHandle;

/**
 * @author Jan Wiemer
 *
 * Listener that gets notified on each modification during commit.
 * 
 * @param <K> Key type of the store entry
 * @param <V> Value type of the store entry
 */
public interface JacisModificationListener<K, V> {

  void onModification(K key, V oldValue, V newValue, JacisTransactionHandle tx);

}
