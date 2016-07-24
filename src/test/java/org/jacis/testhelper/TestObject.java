/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.testhelper;

import org.jacis.plugin.objectadapter.cloning.JacisCloneable;
import org.jacis.plugin.objectadapter.readonly.AbstractReadOnlyModeSupportingObject;

import java.io.Serializable;

/**
 * @author Jan Wiemer
 *
 */
public class TestObject extends AbstractReadOnlyModeSupportingObject implements JacisCloneable<TestObject>, Serializable {

  private static final long serialVersionUID = 1L;

  private String name;
  private long value;
  private String strValue;

  public TestObject(String name, long value) {
    this.name = name;
    this.value = value;
    strValue = null;
  }

  public TestObject(String name) {
    this(name, 1);
  }

  @Override
  public TestObject clone() {
    return (TestObject) super.clone();
  }

  public String getName() {
    return name;
  }

  public TestObject setName(String name) {
    checkWritable();
    this.name = name;
    return this;
  }

  public long getValue() {
    return value;
  }

  public TestObject setValue(long value) {
    checkWritable();
    this.value = value;
    return this;
  }

  public String getStrValue() {
    return strValue;
  }

  public TestObject setStrValue(String strValue) {
    this.strValue = strValue;
    return this;
  }

  @Override
  public String toString() {
    StringBuilder b = new StringBuilder();
    b.append(getClass().getSimpleName());
    b.append("(").append(name).append(":").append(value);
    if (strValue != null) {
      b.append(", strVal=").append(strValue);
    }
    b.append(")");
    return b.toString();
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
    TestObject other = (TestObject) obj;
    if (name == null) {
      if (other.name != null)
        return false;
    } else if (!name.equals(other.name))
      return false;
    return value == other.value;
  }

}
