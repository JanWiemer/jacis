/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.plugin.objectadapter;

/**
 *
 * Object adapter providing the methods how to transfer objects from the store of committed objects to the transaction view and back
 *
 * @param <TV> Type of the objects in the transaction view. This is the type visible from the outside.
 * @param <CV> Type of the objects as they are stored in the internal map of committed values. This type is not visible from the outside.
 * @author Jan Wiemer
 */
public interface JacisObjectAdapter<TV, CV> {

  TV cloneCommitted2WritableTxView(CV value);

  TV cloneCommitted2ReadOnlyTxView(CV value);

  TV cloneTxView2ReadOnlyTxView(TV value);

  CV cloneTxView2Committed(TV value);

}