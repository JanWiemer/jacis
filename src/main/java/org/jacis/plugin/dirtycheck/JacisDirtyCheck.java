/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.plugin.dirtycheck;

import org.jacis.plugin.readonly.object.AbstractReadOnlyModeSupportingObject;

/**
 * This interface provides the possibility to register a dirty check for an object type.
 * The purpose of this check is to mark the object as updated without the need to explicitly call the
 * {@link org.jacis.store.JacisStore#update(Object, Object)} method. If this method returns 'true' the
 * object is marked as updated. Note that it is still possible to mark an object as update manually by
 * calling the 'update' method. For objects already marked as updated the dirty check is not called.
 * <p>
 * Note that the original value passed to the check method can only be used to implement a dirty check
 * if the original value is tracked by the store (see {@link org.jacis.container.JacisObjectTypeSpec#trackOriginalValue})).
 * Otherwise always 'null' will be passed to the original value parameter.
 * <p>
 * Note that the implementation of the method may be based on comparing the properties of the value with
 * the properties of the original value (the classical dirty check), or by setting a dirty flag at the object
 * all the time a modifying method is called. The latter can be combined with the
 * {@link org.jacis.plugin.readonly.object.AbstractReadOnlyModeSupportingObject} implementation of the
 * {@link org.jacis.plugin.readonly.object.JacisReadonlyModeSupport} since objects derived from this class
 * anyway have to call a {@link AbstractReadOnlyModeSupportingObject#checkWritable()} method in each method
 * modifying the object.
 *
 * @param <K>  Key type of the store entry
 * @param <TV> Type of the objects in the transaction view. This is the type visible from the outside.
 * @author Jan Wiemer
 */
public interface JacisDirtyCheck<K, TV> {

  /**
   * Checks if the passed object is dirty.
   * The purpose of this check is to mark the object as updated without the need to explicitly call the
   * {@link org.jacis.store.JacisStore#update(Object, Object)} method. If this method returns 'true' the
   * object is marked as updated. Note that it is still possible to mark an object as update manually by
   * calling the 'update' method. For objects already marked as updated the dirty check is not called.
   *
   * @param key           The key of the object to check.
   * @param originalValue The original value (when the object is cloned to the transactional view;
   *                      if the original value is tracked otherwise null
   *                      (see {@link org.jacis.container.JacisObjectTypeSpec#trackOriginalValue})).
   * @param currentValue  The current value that should be checked if it is dirty.
   * @return if the object is dirty, that is the current value has changed compared to the original value.
   */
  boolean isDirty(K key, TV originalValue, TV currentValue);

}
