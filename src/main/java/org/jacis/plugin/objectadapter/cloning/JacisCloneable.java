/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.plugin.objectadapter.cloning;

public interface JacisCloneable<OT> extends Cloneable {

  OT clone();

}
