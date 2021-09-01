/*
 * Copyright (c) 2020. Jan Wiemer
 */
package org.jacis.persistence.microstream.microstreamframework.typehandler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.jacis.persistence.microstream.microstreamframework.typehandler.arraylist.TestListObject;
import org.jacis.persistence.microstream.microstreamframework.typehandler.arraylist.TestListObjectHandler;
import org.jacis.persistence.microstream.microstreamframework.typehandler.simple.TestObject;
import org.jacis.persistence.microstream.microstreamframework.typehandler.simple.TestObjectHandler;
import org.jacis.testhelper.FileUtils;
import org.junit.AfterClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import one.microstream.storage.embedded.types.EmbeddedStorage;
import one.microstream.storage.embedded.types.EmbeddedStorageManager;

public class CustomTypeHandlerTest {

  private static final Logger log = LoggerFactory.getLogger(CustomTypeHandlerTest.class);

  protected static Path getStorageDir(String suffix) {
    Path path = suffix == null ? Paths.get("var", CustomTypeHandlerTest.class.getName()) : Paths.get("var", CustomTypeHandlerTest.class.getName(), suffix);
    log.info("use storage path: {}", path.toString());
    return path;
  }

  @AfterClass
  public static void cleanup() {
    FileUtils.deleteDirectory(Paths.get("var", CustomTypeHandlerTest.class.getName()).toFile());
  }

  @Test
  public void testCustomTypeHandler() {
    Path storageDir = getStorageDir("testCustomTypeHandler");
    EmbeddedStorageManager storageManager = createStorageManager(storageDir);
    assertNull(storageManager.root());
    storageManager.setRoot(new TestObject("Jacis", "Store"));
    assertTrue(storageManager.root().toString().startsWith("Jacis"));
    storageManager.storeRoot();
    storageManager.shutdown();
    storageManager = createStorageManager(storageDir);
    TestObject root = (TestObject) storageManager.root();
    assertTrue(root.toString().startsWith("Jacis"));
    root.setAge(22);
    storageManager.storeRoot();
    storageManager.shutdown();
  }

  @Test
  public void testCustomListTypeHandler() {
    Path storageDir = getStorageDir("testCustomListTypeHandler");
    EmbeddedStorageManager storageManager = createStorageManager(storageDir);
    assertNull(storageManager.root());
    TestListObject list = new TestListObject(5);
    storageManager.setRoot(list);
    list.add("One");
    list.add("Two");
    list.add("Three");
    log.info("TEST list: {}", list);
    storageManager.storeRoot();
    list.getContent().forEach(storageManager::store);
    storageManager.shutdown();
    EmbeddedStorageManager storageManager2 = createStorageManager(storageDir);
    TestListObject checkList1 = (TestListObject) storageManager2.root();
    log.info("TEST check 1: {}", checkList1);
    assertEquals(3, checkList1.getContent().size());
    assertEquals("One", checkList1.getContent().get(0));
    assertEquals("Two", checkList1.getContent().get(1));
    assertEquals("Three", checkList1.getContent().get(2));
//    checkList1.getContent().set(0, "One New ILLEGAL");
    checkList1.set(1, "Two New");
    storageManager2.storeRoot();
    storageManager2.shutdown();
    EmbeddedStorageManager storageManager3 = createStorageManager(storageDir);
    TestListObject checkList2 = (TestListObject) storageManager3.root();
    log.info("TEST check 2: {}", checkList2);
    assertEquals(3, checkList2.getContent().size());
    assertEquals("One", checkList2.getContent().get(0));
    assertEquals("Two New", checkList2.getContent().get(1));
    assertEquals("Three", checkList2.getContent().get(2));
    storageManager3.storeRoot();
    storageManager3.shutdown();
  }

  @SuppressWarnings("unchecked")
  protected EmbeddedStorageManager createStorageManager(Path storageDir) {
    return EmbeddedStorage.Foundation(storageDir) //
        .onConnectionFoundation(f -> f.registerCustomTypeHandlers(new TestObjectHandler(), new TestListObjectHandler())) //
        .start();
  }

}
