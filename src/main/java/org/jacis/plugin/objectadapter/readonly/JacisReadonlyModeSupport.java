package org.jacis.plugin.objectadapter.readonly;

/**
 * @author Jan Wiemer
 *
 *  Interface objects can implement to support switching them to read only mode and back.
 */
public interface JacisReadonlyModeSupport {

  public void switchToReadOnlyMode();

  public void switchToReadWriteMode();

  public boolean isReadOnly();

  public default void switchToReadOnlyMode(boolean readOnlyMode) {
    if (readOnlyMode) {
      switchToReadOnlyMode();
    } else {
      switchToReadWriteMode();
    }
  }

}
