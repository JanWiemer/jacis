/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.exception;

import org.jacis.JacisApi;
import org.jacis.container.JacisContainer.StoreIdentifier;
import org.jacis.container.JacisTransactionHandle;
import org.jacis.store.JacisStore;
import org.jacis.trackedviews.TrackedView;

/**
 * Exception thrown in case the modification of a tracked view during commit causes an exception.
 *
 * @author Jan Wiemer
 */
@JacisApi
public class JacisTrackedViewModificationException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  private final StoreIdentifier storeIdentifier;
  private final TrackedView<?> view;
  private final String txId;
  private final String txDescription;
  private final Object key;
  private final Object oldValue;
  private final Object newValue;

  public JacisTrackedViewModificationException(JacisStore<?, ?> store, TrackedView<?> view, JacisTransactionHandle transaction, Object key, Object oldValue, Object newValue, Exception e) {
    super(computeMessage(store, view, transaction, key, oldValue, newValue, e), e);
    this.view = view;
    this.key = key;
    this.oldValue = oldValue;
    this.newValue = newValue;
    storeIdentifier = store.getStoreIdentifier();
    txId = transaction.getTxId();
    txDescription = transaction.getTxDescription();
  }

  private static String computeMessage(JacisStore<?, ?> store, TrackedView<?> view, JacisTransactionHandle transaction, Object key, Object oldValue, Object newValue, Exception e) {
    return "Tracking modification for TX " + transaction.getTxId() + " on view " + view + " of store " + store.getStoreIdentifier() + " causes exception: >" + e.toString() + "<!" + //
        "(modifying object with key >" + key + "< from >" + oldValue + "< to >" + newValue + "<)";
  }

  public StoreIdentifier getStoreIdentifier() {
    return storeIdentifier;
  }

  public TrackedView<?> getView() {
    return view;
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
