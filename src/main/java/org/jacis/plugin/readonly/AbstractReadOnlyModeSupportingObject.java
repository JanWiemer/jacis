package org.jacis.plugin.readonly;

/**
 * @author Jan Wiemer
 *
 * Abstract base class for objects supporting switching them to read only mode and back.
 */
public abstract class AbstractReadOnlyModeSupportingObject implements JacisReadonlyModeSupport {

  private Thread threadWithWriteAccess = null;

  public AbstractReadOnlyModeSupportingObject() {
    threadWithWriteAccess = Thread.currentThread(); // when creating the object its writable
  }

  @Override
  public void switchToReadOnlyMode() {
    threadWithWriteAccess = null;
  }

  @Override
  public void switchToReadWriteMode() {
    threadWithWriteAccess = Thread.currentThread();
  }

  protected void checkWritable() {
    if (threadWithWriteAccess == null) {
      throw new IllegalStateException("Object currently in read only mode! Accessing Thread: " + Thread.currentThread() + ". Object: " + this);
    } else if (!threadWithWriteAccess.equals(Thread.currentThread())) {
      throw new IllegalStateException("Object currently only writable for thread " + threadWithWriteAccess + "! Accessing Thread: " + Thread.currentThread() + ". Object: " + this);
    }
  }

}
