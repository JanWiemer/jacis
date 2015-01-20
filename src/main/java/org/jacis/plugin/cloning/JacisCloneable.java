package org.jacis.plugin.cloning;

public interface JacisCloneable<OT> extends Cloneable {

  public OT clone();

}
