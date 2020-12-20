/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.testhelper;

import java.io.Serializable;

import org.jacis.plugin.objectadapter.cloning.JacisCloneable;

/**
 * A JACIS cloneable test object that does *not* provide a read only mode.
 *
 * @author Jan Wiemer
 */
public class TestObjectWithoutReadOnlyMode implements JacisCloneable<TestObjectWithoutReadOnlyMode>, Serializable {

  private static final long serialVersionUID = 1L;

  private String name;
  private long value;

  public TestObjectWithoutReadOnlyMode(String name, long value) {
    this.name = name;
    this.value = value;
  }

  @Override
  public TestObjectWithoutReadOnlyMode clone() {
    try {
      return (TestObjectWithoutReadOnlyMode) super.clone();
    } catch (CloneNotSupportedException e) {
      throw new InternalError("Could not clone " + this.getClass().getName());
    }
  }

  public String getName() {
    return name;
  }

  public TestObjectWithoutReadOnlyMode setName(String name) {
    this.name = name;
    return this;
  }

  public long getValue() {
    return value;
  }

  public TestObjectWithoutReadOnlyMode setValue(long value) {
    this.value = value;
    return this;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(" + name + ":" + value + ")";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    result = prime * result + (int) (value ^ (value >>> 32));
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    TestObjectWithoutReadOnlyMode other = (TestObjectWithoutReadOnlyMode) obj;
    if (name == null) {
      if (other.name != null)
        return false;
    } else if (!name.equals(other.name))
      return false;
    return value == other.value;
  }

}
