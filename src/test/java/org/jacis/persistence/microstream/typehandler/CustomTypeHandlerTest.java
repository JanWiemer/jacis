package org.jacis.persistence.microstream.typehandler;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.jacis.testhelper.FileUtils;
import org.junit.AfterClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import one.microstream.storage.types.EmbeddedStorage;
import one.microstream.storage.types.EmbeddedStorageManager;

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
  public void testCustomTyopeHandler() {
    Path storageDir = getStorageDir("testCustomTyopeHandler");
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

  @SuppressWarnings("unchecked")
  protected EmbeddedStorageManager createStorageManager(Path storageDir) {
    return EmbeddedStorage.Foundation(storageDir) //
        .onConnectionFoundation(f -> f.registerCustomTypeHandlers(new TestObjectHandler())) //
        .start();
  }

}
