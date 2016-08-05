/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.plugin.objectadapter.cloning.readonly;

/**
 * Thrown when trying to modify an object in read only mode.
 */
public class ReadOnlyException extends IllegalStateException {

  ReadOnlyException(String s) {
    super(s);
  }
}
