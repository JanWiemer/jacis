/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.plugin.objectadapter.serialization;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * Generic implementation of the {@link org.jacis.plugin.objectadapter.JacisObjectAdapter} copying the objects
 * to and from the transactional view by means of Java serialization.
 *
 * @param <TV> The object type (note that in this case the committed values and the values in the transactional view have the same type)
 * @author Jan Wiemer
 */
public class JacisJavaSerializationObjectAdapter<TV extends Serializable> extends JacisSerializationObjectAdapter<TV> {

  @Override
  protected byte[] serialize(TV obj) {
    if (obj == null) {
      return null;
    }
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
      try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
        oos.writeObject(obj);
        return bos.toByteArray();
      }
    } catch (IOException e) {
      throw new RuntimeException("Serialization object to byte[] failed! Object: " + obj, e);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  protected TV deserialize(byte[] bytes) {
    if (bytes == null) {
      return null;
    }
    try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes)) {
      try (ObjectInputStream ois = new ObjectInputStream(bis)) {
        return (TV) ois.readObject();
      }
    } catch (IOException | ClassNotFoundException e) {
      throw new RuntimeException("Deserialization object from byte[] failed! Bytes: " + toHexStr(bytes), e);
    }
  }

  private String toHexStr(byte[] bytes) {
    StringBuilder sb = new StringBuilder();
    for (byte b : bytes) {
      sb.append(String.format("%02X", b));
    }
    return sb.toString();
  }
}
