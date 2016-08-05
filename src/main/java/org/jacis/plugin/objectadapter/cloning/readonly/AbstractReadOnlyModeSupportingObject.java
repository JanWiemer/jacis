/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.plugin.objectadapter.cloning.readonly;

import org.jacis.exception.ReadOnlyException;

/**
 * Abstract base class for objects supporting switching them to read only mode and back.
 * @author Jan Wiemer
 */
public abstract class AbstractReadOnlyModeSupportingObject implements JacisReadonlyModeSupport {

  private Thread threadWithWriteAccess = null;

  protected AbstractReadOnlyModeSupportingObject() {
    threadWithWriteAccess = Thread.currentThread(); // when creating the object its writable
  }

  @Override
  protected Object clone() {
    try {
      return super.clone();
    } catch (CloneNotSupportedException e) {
      throw new InternalError("Could not clone " + this.getClass().getName());
    }
  }

  @Override
  public void switchToReadOnlyMode() {
    threadWithWriteAccess = null;
  }

  @Override
  public void switchToReadWriteMode() {
    threadWithWriteAccess = Thread.currentThread();
  }

  @Override
  public boolean isReadOnly() {
    return threadWithWriteAccess == null;
  }

  protected void checkWritable() throws ReadOnlyException {
    if (threadWithWriteAccess == null) {
      throw new ReadOnlyException("Object currently in read only mode! Accessing Thread: " + Thread.currentThread() + ". Object: " + this);
    } else if (!threadWithWriteAccess.equals(Thread.currentThread())) {
      throw new ReadOnlyException("Object currently only writable for thread " + threadWithWriteAccess + "! Accessing Thread: " + Thread.currentThread() + ". Object: " + this);
    }
  }

}
