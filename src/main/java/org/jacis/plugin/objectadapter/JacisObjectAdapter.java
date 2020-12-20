/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.plugin.objectadapter;

/**
 * The object adapter defines how to copy the objects to a transactional view and back.
 *
 * This interface for an object adapter provides the methods how to transfer objects
 * from the store of committed objects to the transaction view and back.
 * The object adapter can be defined individually for each store, that is each object type.
 * The actual implementation is stored in the {@link org.jacis.container.JacisObjectTypeSpec} for a store
 * (see {@link org.jacis.container.JacisObjectTypeSpec#objectAdapter}).
 *
 * The type parameters of this interface refer the type / class of the committed objects and the type / class
 * of the objects in the transactional view. The latter is the type the objects have for the outside world.
 * Depending on the implementation of the object adapter both types can be the same or different
 * (if e.g. the stored values are stored in a somehow compressed form).
 *
 * There are two generic default implementations for an object that can be used:
 * * One default implementation copies the objects by cloning them using the Java 'clone' method
 * (see {@link org.jacis.plugin.objectadapter.cloning.JacisCloningObjectAdapter}).
 * * The other default implementation copies the objects by Java serialization
 * (see {@link org.jacis.plugin.objectadapter.serialization.JacisJavaSerializationObjectAdapter}).
 *
 * @param <TV> Type of the objects in the transaction view. This is the type visible from the outside.
 * @param <CV> Type of the objects as they are stored in the internal map of committed values. This type is not visible from the outside.
 * @author Jan Wiemer
 */
public interface JacisObjectAdapter<TV, CV> {

  /**
   * Clone a committed version of the object into the transactional view.
   * Note that modifications ib the returned transactional view must not influence the committed version of the object.
   *
   * @param value Committed version of the object.
   * @return A transactional view for the passed object.
   */
  TV cloneCommitted2WritableTxView(CV value);

  /**
   * Clone back a transactional view of the object into the committed version of the object.
   * The returned committed version will replace the previous one in the store of the committed values.
   *
   * @param value A transactional view for the passed object.
   * @return Committed version of the object.
   */
  CV cloneTxView2Committed(TV value);

  /**
   * Clone a committed version of the object into a read only transactional view.
   * Modifications of the read only view must not be possible (lead to an exception).
   * As an optimization it is allowed to skip a real cloning here since the returned object is guaranteed to be read only.
   * Note that a difference using read only views where cloning is skipped is that a repeated read of the same object
   * may return different versions of the object if another transaction committed a newer version between both reads.
   * 
   * @param value Committed version of the object.
   * @return A read only transactional view for the passed object.
   */
  TV cloneCommitted2ReadOnlyTxView(CV value);

  /**
   * Convert a transactional view of an object into a read only representation of this object.
   * Modifications of the read only view must not be possible (lead to an exception).
   * 
   * @param value A transactional view for the passed object.
   * @return A read only transactional view for the passed object.
   */
  TV cloneTxView2ReadOnlyTxView(TV value);

}
