/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.plugin.persistence;

import org.jacis.plugin.JacisModificationListener;
import org.jacis.store.JacisStoreImpl;

/**
 * @author Jan Wiemer
 */
public interface JacisPersistenceAdapter<K, V> extends JacisModificationListener<K, V> {

  void initializeStore(JacisStoreImpl<K, V, ?> store);

  void prepareCommit();

  void commit();

  void rollback();

}
