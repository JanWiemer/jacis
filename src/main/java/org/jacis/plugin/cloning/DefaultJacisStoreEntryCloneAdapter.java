package org.jacis.plugin.cloning;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class DefaultJacisStoreEntryCloneAdapter<V> implements JacisStoreEntryCloneAdapter<V> {

  @Override
  @SuppressWarnings("unchecked")
  public V cloneValue(V value) {
    if (value == null) {
      return null;
    }
    V clone;
    if (value instanceof JacisCloneable) {
      clone = (V) ((JacisCloneable<V>) value).clone();
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
    } catch (SecurityException e) {
      throw new IllegalArgumentException("Failed to clone object " + obj + "! Clone method not accessible: " + e, e);
    } catch (IllegalAccessException e) {
      throw new IllegalArgumentException("Failed to clone object " + obj + "! Clone method not accessible: " + e, e);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Failed to clone object " + obj + "! Clone method not accessible: " + e, e);
    } catch (InvocationTargetException e) {
      throw new IllegalArgumentException("Failed to clone object " + obj + "! Clone method not accessible: " + e, e);
    }
  }

}
