package org.jacis.plugin.objectadapter.cloning;

public interface JacisCloneable<OT> extends Cloneable {

  public OT clone();

}
