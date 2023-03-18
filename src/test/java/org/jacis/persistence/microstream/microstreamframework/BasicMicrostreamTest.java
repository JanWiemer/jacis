/*
 * Copyright (c) 2020. Jan Wiemer
 */

package org.jacis.persistence.microstream.microstreamframework;

import one.microstream.storage.embedded.configuration.types.EmbeddedStorageConfigurationBuilder;
import one.microstream.storage.embedded.types.EmbeddedStorage;
import one.microstream.storage.embedded.types.EmbeddedStorageManager;
import org.jacis.testhelper.FileUtils;
import org.junit.AfterClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.*;

public class BasicMicrostreamTest {

  private static final Logger log = LoggerFactory.getLogger(BasicMicrostreamTest.class);

  protected static Path getStorageDir(String suffix) {
    Path path = suffix == null ? Paths.get("var", BasicMicrostreamTest.class.getName()) : Paths.get("var", BasicMicrostreamTest.class.getName(), suffix);
    log.info("use storage path: {}", path);
    return path;
  }

  @AfterClass
  public static void cleanup() {
    FileUtils.deleteDirectory(Paths.get("var", BasicMicrostreamTest.class.getName()).toFile());
  }

  @Test
  public void testEmptyStore() {
    Path storageDir = getStorageDir("testEmptyStore");
    EmbeddedStorageManager storageManager = EmbeddedStorage.start(storageDir);
    assertNull(storageManager.root());
    storageManager.shutdown();
    storageManager = EmbeddedStorage.start(storageDir);
    assertNull(storageManager.root());
    storageManager.shutdown();
  }

  @Test
  public void testInsertWithoutStoreRoot() {
    Path storageDir = getStorageDir("testInsertWithoutStoreRoot");
    EmbeddedStorageManager storageManager = EmbeddedStorage.start(storageDir);
    assertNull(storageManager.root());
    storageManager.setRoot("Hello World! @ " + new Date());
    assertTrue(storageManager.root().toString().startsWith("Hello World"));
    storageManager.shutdown();
    storageManager = EmbeddedStorage.start(storageDir);
    assertNull(storageManager.root());
    storageManager.shutdown();
  }

  @Test
  public void testInsertAndStoreRoot() {
    Path storageDir = getStorageDir("testInsertAndStoreRoot");
    EmbeddedStorageManager storageManager = EmbeddedStorage.start(storageDir);
    assertNull(storageManager.root());
    storageManager.setRoot("Hello World! @ " + new Date());
    storageManager.storeRoot();
    assertTrue(storageManager.root().toString().startsWith("Hello World"));
    storageManager.shutdown();
    storageManager = EmbeddedStorage.start(storageDir);
    log.info("---all routes:---");
    storageManager.viewRoots().iterateEntries((s, o) -> log.info("root({}): {}", s, o));
    assertTrue(storageManager.root().toString().startsWith("Hello World"));
    storageManager.shutdown();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testInsertAndStoreArrayRoot() {
    Path storageDir = getStorageDir("testInsertAndStoreArrayRoot");
    EmbeddedStorageManager storageManager = EmbeddedStorageConfigurationBuilder.New() //
        .setStorageDirectory(storageDir.toString()) //
        .createEmbeddedStorageFoundation() //
        .createEmbeddedStorageManager();
    log.info("initialize data with 5 elements...");
    storageManager.start();
    List<String> data = new ArrayList<>(Arrays.asList("Element 1", "Element 2", "Element 3", "Element 4", "Element 5"));
    storageManager.setRoot(data);
    storageManager.storeRoot();
    storageManager.shutdown();
    //
    log.info("check stored data with 5 elements...");
    storageManager = EmbeddedStorage.start(storageDir);
    List<String> checkData = (List<String>) storageManager.root();
    checkData.forEach(d -> log.info("data: {}", d));
    assertEquals(data, checkData);
    log.info("remove one element from stored data...");
    checkData.remove(2);
    storageManager.storeRoot();
    storageManager.shutdown();
    //
    log.info("check stored data now with 4 elements...");
    storageManager = EmbeddedStorage.start(storageDir);
    List<String> checkData2 = (List<String>) storageManager.root();
    checkData.forEach(d -> log.info("data: {}", d));
    assertEquals(checkData, checkData2);
    storageManager.shutdown();
  }

}
