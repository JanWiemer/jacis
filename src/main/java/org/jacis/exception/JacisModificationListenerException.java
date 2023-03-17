/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.exception;

import org.jacis.JacisApi;
import org.jacis.container.JacisContainer.StoreIdentifier;
import org.jacis.container.JacisTransactionHandle;
import org.jacis.plugin.JacisModificationListener;
import org.jacis.store.JacisStore;

/**
 * Exception thrown in case the modification of a tracked view during commit causes an exception.
 *
 * @author Jan Wiemer
 */
@SuppressWarnings({"unused", "UnusedReturnValue"}) // since this is an API of the library
@JacisApi
public class JacisModificationListenerException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  private final StoreIdentifier storeIdentifier;
  private final JacisModificationListener<?, ?> listener;
  private final String txId;
  private final String txDescription;
  private final Object key;
  private final Object oldValue;
  private final Object newValue;

  public JacisModificationListenerException(JacisStore<?, ?> store, JacisModificationListener<?, ?> listener, JacisTransactionHandle transaction, Object key, Object oldValue, Object newValue, Exception e) {
    super(computeMessage(store, listener, transaction, key, oldValue, newValue, e), e);
    this.listener = listener;
    this.key = key;
    this.oldValue = oldValue;
    this.newValue = newValue;
    storeIdentifier = store.getStoreIdentifier();
    txId = transaction.getTxId();
    txDescription = transaction.getTxDescription();
  }

  private static String computeMessage(JacisStore<?, ?> store, JacisModificationListener<?, ?> listener, JacisTransactionHandle transaction, Object key, Object oldValue, Object newValue, Exception e) {
    return "Tracking modification for TX " + transaction.getTxId() + " on listener " + listener + " of store " + store.getStoreIdentifier() + " causes exception: >" + e.toString() + "<!" + //
        "(modifying object with key >" + key + "< from >" + oldValue + "< to >" + newValue + "<)";
  }

  public StoreIdentifier getStoreIdentifier() {
    return storeIdentifier;
  }

  public JacisModificationListener<?, ?> getListener() {
    return listener;
  }

  public String getTxId() {
    return txId;
  }

  public String getTxDescription() {
    return txDescription;
  }

  public Object getKey() {
    return key;
  }

  public Object getOldValue() {
    return oldValue;
  }

  public Object getNewValue() {
    return newValue;
  }

}
