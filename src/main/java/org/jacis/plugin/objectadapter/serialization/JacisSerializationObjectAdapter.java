/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.plugin.objectadapter.serialization;

import org.jacis.plugin.objectadapter.JacisObjectAdapter;

import java.io.Serializable;

/**
 * Abstract generic implementation of the {@link org.jacis.plugin.objectadapter.JacisObjectAdapter} copying the objects
 * to and from the transactional view by means of a serialization mechanism.
 * Serialization and de-serialization of the objects is delegated to the abstract methods
 * {@link #serialize(Serializable)} and {@link #deserialize(byte[])}.
 *
 * @param <TV> The object type (note that in this case the committed values and the values in the transactional view have the same type)
 * @author Jan Wiemer
 */
public abstract class JacisSerializationObjectAdapter<TV extends Serializable> implements JacisObjectAdapter<TV, byte[]> {

  @Override
  public TV cloneCommitted2WritableTxView(byte[] bytes) {
    return deserialize(bytes);
  }

  @Override
  public TV cloneCommitted2ReadOnlyTxView(byte[] bytes) {
    return deserialize(bytes);
  }

  @Override
  public TV cloneTxView2ReadOnlyTxView(TV value) {
    return deserialize(serialize(value));
  }

  @Override
  public byte[] cloneTxView2Committed(TV value) {
    return serialize(value);
  }

  /**
   * Serialize the passed object to a byte array.
   *
   * @param obj The object to serialize.
   * @return The bytes of the serialized object.
   */
  protected abstract byte[] serialize(TV obj);

  /**
   * De-serialize an object from the passed byte array.
   * 
   * @param bytes The bytes from which to de-serialize the object.
   * @return The de-serialized object.
   */
  protected abstract TV deserialize(byte[] bytes);

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }
}
