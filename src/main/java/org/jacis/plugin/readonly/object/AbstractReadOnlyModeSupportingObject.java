/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.plugin.readonly.object;

import org.jacis.exception.ReadOnlyException;

/**
 * Abstract base class for objects supporting switching them between the usual read-write mode and a read-only mode.
 * Therefore it implements the methods {@link #switchToReadOnlyMode()} and {@link #switchToReadWriteMode()} from
 * the {@link JacisReadonlyModeSupport} interface. Note that all the time only one single thread is allowed to
 * have write access to the object. When switching an object to read-write mode the current thread is stored
 * as thread with write access (see {@link #threadWithWriteAccess}).
 *
 * For actual implementations the class provides the protected method {@link #checkWritable()}.
 * This method should be called prior to all modifying accesses to the object (e.g. in al setter-methods).
 * The method will throw a {@link ReadOnlyException} if the current thread has no write access to the object.
 *
 * @author Jan Wiemer
 */
public abstract class AbstractReadOnlyModeSupportingObject implements JacisReadonlyModeSupport {

  /** The thread currently permitted to modify the object (if any) */
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

  /**
   * This method should be called prior to all modifying accesses to the object (e.g. in al setter-methods).
   * The method will check if the current thread has write access to the object and will throw a {@link ReadOnlyException} otherwise.
   * @throws ReadOnlyException thrown if the current thread has no write access to the object.
   */
  protected void checkWritable() throws ReadOnlyException {
    if (threadWithWriteAccess == null) {
      throw new ReadOnlyException("Object currently in read only mode! Accessing Thread: " + Thread.currentThread() + ". Object: " + this);
    } else if (!threadWithWriteAccess.equals(Thread.currentThread())) {
      throw new ReadOnlyException("Object currently only writable for thread " + threadWithWriteAccess + "! Accessing Thread: " + Thread.currentThread() + ". Object: " + this);
    }
  }

}
