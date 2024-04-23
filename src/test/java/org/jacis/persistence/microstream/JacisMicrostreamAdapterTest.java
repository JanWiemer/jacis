/*
 * Copyright (c) 2020. Jan Wiemer
 */
package org.jacis.persistence.microstream;

import org.eclipse.store.storage.embedded.configuration.types.EmbeddedStorageConfigurationBuilder;
import org.eclipse.store.storage.embedded.types.EmbeddedStorageManager;
import org.jacis.container.JacisContainer;
import org.jacis.container.JacisObjectTypeSpec;
import org.jacis.extension.persistence.microstream.MicrostreamPersistenceAdapter;
import org.jacis.extension.persistence.microstream.MicrostreamStorage;
import org.jacis.plugin.objectadapter.cloning.JacisCloningObjectAdapter;
import org.jacis.plugin.txadapter.local.JacisLocalTransaction;
import org.jacis.store.JacisStore;
import org.jacis.testhelper.FileUtils;
import org.jacis.testhelper.JacisTestHelper;
import org.jacis.testhelper.TestObject;
import org.junit.AfterClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class JacisMicrostreamAdapterTest {

  private static final Logger log = LoggerFactory.getLogger(JacisMicrostreamAdapterTest.class);

  protected static Path getStorageDir(String suffix) {
    Path path = suffix == null ? Paths.get("var", JacisMicrostreamAdapterTest.class.getName()) : Paths.get("var", JacisMicrostreamAdapterTest.class.getName(), suffix);
    log.info("use storage path: {}", path);
    return path;
  }

  @AfterClass
  public static void cleanup() {
    FileUtils.deleteDirectory(Paths.get("var", JacisMicrostreamAdapterTest.class.getName()).toFile());
  }

  private EmbeddedStorageManager createStorageManager(Path storageDir) {
    EmbeddedStorageManager storageManager = EmbeddedStorageConfigurationBuilder.New() //
        .setStorageDirectory(storageDir.toString()) //
        .createEmbeddedStorageFoundation() //
        .createEmbeddedStorageManager();
    storageManager.start();
    return storageManager;
  }

  private JacisStore<String, TestObject> createTestStore(EmbeddedStorageManager storageManager) {
    JacisTestHelper testHelper = new JacisTestHelper();
    JacisContainer container = testHelper.createTestContainer();
    JacisCloningObjectAdapter<TestObject> cloningAdapter = new JacisCloningObjectAdapter<>();
    JacisObjectTypeSpec<String, TestObject, TestObject> objectTypeSpec = new JacisObjectTypeSpec<>(String.class, TestObject.class, cloningAdapter);
    MicrostreamStorage storage = new MicrostreamStorage(storageManager);
    objectTypeSpec.setPersistenceAdapter(new MicrostreamPersistenceAdapter<>(storage));
    container.createStore(objectTypeSpec);
    return container.getStore(String.class, TestObject.class);
  }

  @Test
  @Ignore
  public void testRestartWithEmptyStore() {
    Path storageDir = getStorageDir("testRestartWithEmptyStore");
    EmbeddedStorageManager storageManager = createStorageManager(storageDir);
    JacisStore<String, TestObject> store = createTestStore(storageManager);
    assertEquals(0, store.size());
    // RESTART
    storageManager.shutdown();
    storageManager = createStorageManager(storageDir);
    store = createTestStore(storageManager);
    //
    JacisLocalTransaction readingTx = store.getContainer().beginLocalTransaction();
    assertEquals(0, store.size());
    readingTx.commit();
  }

  @Test
  @Ignore
  public void testRestartWithOneElement() {
    Path storageDir = getStorageDir("testRestartWithOneElement");
    log.info("===== START =====");
    EmbeddedStorageManager storageManager = createStorageManager(storageDir);
    JacisStore<String, TestObject> store = createTestStore(storageManager);
    // create and fill store
    JacisLocalTransaction initTx = store.getContainer().beginLocalTransaction();
    String testObjectName = "obj-1";
    TestObject testObject = new TestObject(testObjectName, 1);
    store.update(testObjectName, testObject);
    initTx.commit();
    assertEquals(1, store.size());
    // RESTART
    log.info("===== RESTART =====");
    storageManager.shutdown();
    storageManager = createStorageManager(storageDir);
    store = createTestStore(storageManager);
    //
    JacisLocalTransaction readingTx = store.getContainer().beginLocalTransaction();
    assertEquals(1, store.size());
    assertEquals(1, store.get(testObjectName).getValue()); // ===== READ => committed value ====
    readingTx.commit();
    log.info("===== FINISHED =====");
  }

  @Test
  @Ignore
  public void testRestartWithMultipleElements() {
    Path storageDir = getStorageDir("testRestartWithMultipleElements");
    EmbeddedStorageManager storageManager = createStorageManager(storageDir);
    JacisStore<String, TestObject> store = createTestStore(storageManager);
    // create and fill store
    JacisLocalTransaction initTx = store.getContainer().beginLocalTransaction();
    store.update("obj-1", new TestObject("obj-1", 1));
    store.update("obj-2", new TestObject("obj-2", 2));
    store.update("obj-3", new TestObject("obj-3", 3));
    store.update("obj-4", new TestObject("obj-4", 4));
    store.update("obj-5", new TestObject("obj-5", 5));
    initTx.commit();
    assertEquals(5, store.size());
    // RESTART
    storageManager.shutdown();
    storageManager = createStorageManager(storageDir);
    store = createTestStore(storageManager);
    //
    JacisLocalTransaction readingTx = store.getContainer().beginLocalTransaction();
    assertEquals(5, store.size());
    assertEquals(1, store.get("obj-1").getValue());
    assertEquals(2, store.get("obj-2").getValue());
    assertEquals(3, store.get("obj-3").getValue());
    assertEquals(4, store.get("obj-4").getValue());
    assertEquals(5, store.get("obj-5").getValue());
    readingTx.commit();
  }

  @Test
  @Ignore
  public void testRestartWithUpdate() {
    Path storageDir = getStorageDir("testRestartWithUpdate");
    EmbeddedStorageManager storageManager = createStorageManager(storageDir);
    JacisStore<String, TestObject> store = createTestStore(storageManager);
    // create and fill store
    JacisLocalTransaction initTx = store.getContainer().beginLocalTransaction();
    store.update("obj-1", new TestObject("obj-1", 1));
    store.update("obj-2", new TestObject("obj-2", 2));
    store.update("obj-3", new TestObject("obj-3", 3));
    store.update("obj-4", new TestObject("obj-4", 4));
    store.update("obj-5", new TestObject("obj-5", 5));
    initTx.commit();
    assertEquals(5, store.size());
    JacisLocalTransaction updateTx = store.getContainer().beginLocalTransaction();
    store.update("obj-3", store.get("obj-3").setValue(13));
    updateTx.commit();
    // RESTART
    storageManager.shutdown();
    storageManager = createStorageManager(storageDir);
    store = createTestStore(storageManager);
    //
    JacisLocalTransaction readingTx = store.getContainer().beginLocalTransaction();
    assertEquals(5, store.size());
    assertEquals(1, store.get("obj-1").getValue());
    assertEquals(2, store.get("obj-2").getValue());
    assertEquals(13, store.get("obj-3").getValue());
    assertEquals(4, store.get("obj-4").getValue());
    assertEquals(5, store.get("obj-5").getValue());
    readingTx.commit();
  }

  @Test
  @Ignore
  public void testRestartWithAdd() {
    Path storageDir = getStorageDir("testRestartWithAdd");
    EmbeddedStorageManager storageManager = createStorageManager(storageDir);
    JacisStore<String, TestObject> store = createTestStore(storageManager);
    // create and fill store
    JacisLocalTransaction initTx = store.getContainer().beginLocalTransaction();
    store.update("obj-1", new TestObject("obj-1", 1));
    store.update("obj-2", new TestObject("obj-2", 2));
    store.update("obj-3", new TestObject("obj-3", 3));
    store.update("obj-4", new TestObject("obj-4", 4));
    store.update("obj-5", new TestObject("obj-5", 5));
    initTx.commit();
    assertEquals(5, store.size());
    JacisLocalTransaction updateTx = store.getContainer().beginLocalTransaction();
    store.update("obj-6", new TestObject("obj-6", 6));
    updateTx.commit();
    assertEquals(6, store.size());
    // RESTART
    storageManager.shutdown();
    storageManager = createStorageManager(storageDir);
    store = createTestStore(storageManager);
    //
    JacisLocalTransaction readingTx = store.getContainer().beginLocalTransaction();
    assertEquals(6, store.size());
    assertEquals(1, store.get("obj-1").getValue());
    assertEquals(2, store.get("obj-2").getValue());
    assertEquals(3, store.get("obj-3").getValue());
    assertEquals(4, store.get("obj-4").getValue());
    assertEquals(5, store.get("obj-5").getValue());
    assertEquals(6, store.get("obj-6").getValue());
    readingTx.commit();
  }

  @Test
  @Ignore
  public void testRestartWithDelete() {
    Path storageDir = getStorageDir("testRestartWithDelete");
    EmbeddedStorageManager storageManager = createStorageManager(storageDir);
    JacisStore<String, TestObject> store = createTestStore(storageManager);
    // create and fill store
    JacisLocalTransaction initTx = store.getContainer().beginLocalTransaction();
    store.update("obj-1", new TestObject("obj-1", 1));
    store.update("obj-2", new TestObject("obj-2", 2));
    store.update("obj-3", new TestObject("obj-3", 3));
    store.update("obj-4", new TestObject("obj-4", 4));
    store.update("obj-5", new TestObject("obj-5", 5));
    initTx.commit();
    assertEquals(5, store.size());
    JacisLocalTransaction updateTx = store.getContainer().beginLocalTransaction();
    store.remove("obj-3");
    updateTx.commit();
    // RESTART
    storageManager.shutdown();
    storageManager = createStorageManager(storageDir);
    store = createTestStore(storageManager);
    //
    JacisLocalTransaction readingTx = store.getContainer().beginLocalTransaction();
    assertEquals(4, store.size());
    assertEquals(1, store.get("obj-1").getValue());
    assertEquals(2, store.get("obj-2").getValue());
    assertNull(store.get("obj-3"));
    assertEquals(4, store.get("obj-4").getValue());
    assertEquals(5, store.get("obj-5").getValue());
    readingTx.commit();
  }

  @Test
  @Ignore
  public void testRestartDeleteRestart() {
    Path storageDir = getStorageDir("testRestartDeleteRestart");
    EmbeddedStorageManager storageManager = createStorageManager(storageDir);
    JacisStore<String, TestObject> store = createTestStore(storageManager);
    // create and fill store
    JacisLocalTransaction initTx = store.getContainer().beginLocalTransaction();
    store.update("obj-1", new TestObject("obj-1", 1));
    store.update("obj-2", new TestObject("obj-2", 2));
    store.update("obj-3", new TestObject("obj-3", 3));
    store.update("obj-4", new TestObject("obj-4", 4));
    store.update("obj-5", new TestObject("obj-5", 5));
    initTx.commit();
    assertEquals(5, store.size());
    // RESTART
    storageManager.shutdown();
    storageManager = createStorageManager(storageDir);
    store = createTestStore(storageManager);
    JacisLocalTransaction updateTx = store.getContainer().beginLocalTransaction();
    store.remove("obj-3");
    updateTx.commit();
    // RESTART
    storageManager.shutdown();
    storageManager = createStorageManager(storageDir);
    store = createTestStore(storageManager);
    //
    JacisLocalTransaction readingTx = store.getContainer().beginLocalTransaction();
    assertEquals(4, store.size());
    assertEquals(1, store.get("obj-1").getValue());
    assertEquals(2, store.get("obj-2").getValue());
    assertNull(store.get("obj-3"));
    assertEquals(4, store.get("obj-4").getValue());
    assertEquals(5, store.get("obj-5").getValue());
    readingTx.commit();
  }

}
