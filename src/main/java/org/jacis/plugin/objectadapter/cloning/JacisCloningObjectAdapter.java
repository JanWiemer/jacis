/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.plugin.objectadapter.cloning;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.jacis.JacisApi;
import org.jacis.plugin.readonly.DefaultJacisStoreEntryReadOnlyModeAdapter;
import org.jacis.plugin.readonly.JacisStoreEntryReadOnlyModeAdapter;

/**
 * Implementation of the {@link AbstractJacisCloningObjectAdapter} cloning the objects based on Java serialization.
 * If the object is an instance of the {@link JacisCloneable} interface the {@link JacisCloneable#clone()} method
 * declared in this interface is used to clone the object.
 * Otherwise, the object may be cloneable (overwrites the {@link Object#clone()} methods)
 * but does not implement the {@link JacisCloneable} interface.
 * In this case the {@link Object#clone()} method is called by reflection.
 *
 * @param <V> The object type (note that in this case the committed values and the values in the transactional view have the same type)
 * @author Jan Wiemer
 */
@JacisApi
public class JacisCloningObjectAdapter<V> extends AbstractJacisCloningObjectAdapter<V> {

  /**
   * Create a cloning object adapter with the passed read only mode adapter.
   * 
   * @param readOnlyModeAdapters Adapter to switch an object between read-only and read-write mode (if supported).
   */
  public JacisCloningObjectAdapter(JacisStoreEntryReadOnlyModeAdapter<V> readOnlyModeAdapters) {
    super(readOnlyModeAdapters);
  }

  /**
   * Create a cloning object adapter with a default read only mode adapter (see {@link DefaultJacisStoreEntryReadOnlyModeAdapter}).
   */
  public JacisCloningObjectAdapter() {
    super();
  }

  @SuppressWarnings("unchecked")
  @Override
  protected V cloneValue(V value) {
    if (value == null) {
      return null;
    }
    V clone;
    if (value instanceof JacisCloneable) {
      clone = ((JacisCloneable<V>) value).clone();
    } else {
      clone = cloneByReflection(value);
    }
    return clone;
  }

  @SuppressWarnings("unchecked")
  private V cloneByReflection(V obj) {
    try {
      Method cloneMethod = obj.getClass().getDeclaredMethod("clone");
      return (V) cloneMethod.invoke(obj);
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException("Failed to clone object " + obj + "! No clone method declared: " + e, e);
    } catch (SecurityException | IllegalAccessException | InvocationTargetException | IllegalArgumentException e) {
      throw new IllegalArgumentException("Failed to clone object " + obj + "! Clone method not accessible: " + e, e);
    }
  }

}
