package org.jacis.plugin.objectadapter.cloning.readonly;

/**
 * @author Jan Wiemer
 *
 *  Interface objects can implement to support switching them to read only mode and back.
 */
public interface JacisReadonlyModeSupport {

  public void switchToReadOnlyMode();

  public void switchToReadWriteMode();

}
