package org.jacis.plugin.objectadapter;

/**
 * @author Jan Wiemer
 * 
 * Object adapter providing the methods how to transfer objects from the store of committed objects to the transaction view and back
 *
 * @param <TV> Type of the objects in the transaction view. This is the type visible from the outside.
 * @param <CV> Type of the objects as they are stored in the internal map of committed values. This type is not visible from the outside.
 */
public interface JacisObjectAdapter<TV, CV> {

  public abstract TV cloneCommitted2WritableTxView(CV value);

  public abstract TV cloneCommitted2ReadOnlyTxView(CV value);

  public abstract TV cloneTxView2ReadOnlyTxView(TV value);

  public abstract CV cloneTxView2Committed(TV value);

}