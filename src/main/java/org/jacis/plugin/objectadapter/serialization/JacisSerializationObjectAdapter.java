package org.jacis.plugin.objectadapter.serialization;

import java.io.Serializable;

import org.jacis.plugin.objectadapter.JacisObjectAdapter;

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

  protected abstract byte[] serialize(TV obj);

  protected abstract TV deserialize(byte[] bytes);

}
