/*
 * Copyright (c) 2020. Jan Wiemer
 */
package org.jacis.testhelper;

import java.io.File;

public abstract class FileUtils {

  public static boolean deleteDirectory(File directoryToBeDeleted) {
    File[] allContents = directoryToBeDeleted.listFiles();
    if (allContents != null) {
      for (File file : allContents) {
        deleteDirectory(file);
      }
    }
    return directoryToBeDeleted.delete();
  }

}
