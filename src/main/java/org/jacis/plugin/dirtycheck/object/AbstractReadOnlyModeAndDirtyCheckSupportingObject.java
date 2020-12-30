/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.plugin.dirtycheck.object;

import org.jacis.JacisApi;
import org.jacis.exception.ReadOnlyException;
import org.jacis.plugin.readonly.object.AbstractReadOnlyModeSupportingObject;

/**
 * Abstract base class for objects supporting switching them between the usual read-write mode and a read-only mode
 * (see {@link AbstractReadOnlyModeSupportingObject})
 * and tracking if the object is dirty by tracking if the {@link #checkWritable()} method has been called.
 *
 * @author Jan Wiemer
 */
@JacisApi
public abstract class AbstractReadOnlyModeAndDirtyCheckSupportingObject extends AbstractReadOnlyModeSupportingObject implements JacisDirtyTrackingObject {

  private boolean dirty = false;

  /** @return if the object is dirty */
  @Override
  public boolean isDirty() {
    return dirty;
  }

  @Override
  protected void checkWritable() throws ReadOnlyException {
    super.checkWritable();
    dirty = true;
  }

  @Override
  public void switchToReadOnlyMode() {
    super.switchToReadOnlyMode();
    dirty = false;
  }

}
