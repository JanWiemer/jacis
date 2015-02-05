package org.jacis.serialization;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicInteger;

import org.jacis.store.JacisStore;
import org.jacis.testhelper.JacisTestHelper;
import org.jacis.testhelper.TestObject;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JacisStoreWithSerializationAdapterTest {

  private static final Logger log = LoggerFactory.getLogger(JacisStoreWithSerializationAdapterTest.class);

  @Test
  public void testInsert() {
    JacisStore<String, TestObject, byte[]> store = new JacisTestHelper().createTestStoreWithSerialization();
    TestObject testObject = new TestObject("obj-1", 1);
    assertEquals(0, store.size());
    store.getContainer().withLocalTx(() -> {
      store.update(testObject.getName(), testObject);
    });
    assertEquals(1, store.size());
    log.info("store={}", store);
  }

  @Test
  public void testInsertReadSameTx() {
    JacisStore<String, TestObject, byte[]> store = new JacisTestHelper().createTestStoreWithSerialization();
    TestObject testObject = new TestObject("obj-1", 1);
    assertEquals(0, store.size());
    store.getContainer().withLocalTx(() -> {
      store.update(testObject.getName(), testObject);
      assertEquals(testObject, store.get(testObject.getName()));
      assertTrue(store.containsKey(testObject.getName()));
    });
    assertEquals(1, store.size());
  }

  @Test
  public void testInsertReadDifferentTx() {
    JacisStore<String, TestObject, byte[]> store = new JacisTestHelper().createTestStoreWithSerialization();
    TestObject testObject = new TestObject("obj-1", 1);
    assertEquals(0, store.size());
    store.getContainer().withLocalTx(() -> {
      store.update(testObject.getName(), testObject);
      assertEquals(testObject, store.get(testObject.getName()));
    });
    assertEquals(1, store.size());
    store.getContainer().withLocalTx(() -> {
      assertEquals(testObject, store.get(testObject.getName()));
      assertTrue(store.containsKey(testObject.getName()));
    });
    assertEquals(1, store.size());
  }

  @Test
  public void testInsertUpdateReadSameTx() {
    JacisStore<String, TestObject, byte[]> store = new JacisTestHelper().createTestStoreWithSerialization();
    TestObject testObject = new TestObject("obj-1", 1);
    assertEquals(0, store.size());
    store.getContainer().withLocalTx(() -> {
      store.update(testObject.getName(), testObject);
      testObject.setValue(2);
      assertEquals(testObject, store.get(testObject.getName()));
      assertEquals(2, store.get(testObject.getName()).getValue());
    });
    assertEquals(1, store.size());
  }

  @Test
  public void testInsertUpdateReadDifferentTx() {
    JacisStore<String, TestObject, byte[]> store = new JacisTestHelper().createTestStoreWithSerialization();
    String testObjectName = "obj-1";
    TestObject testObject = new TestObject(testObjectName, 1);
    assertEquals(0, store.size());
    store.getContainer().withLocalTx(() -> {
      store.update(testObjectName, testObject);
      assertEquals(testObject, store.get(testObjectName));
    });
    store.getContainer().withLocalTx(() -> {
      TestObject testObject2 = store.get(testObjectName);
      assertEquals(testObject, testObject2);
      assertNotSame(testObject, testObject2);
      assertEquals(1, testObject2.getValue());
      testObject2.setValue(2);
      store.update(testObjectName, testObject2);
      assertEquals(2, testObject2.getValue());
    });
    assertEquals(1, store.size());
    store.getContainer().withLocalTx(() -> {
      assertEquals(testObjectName, store.get(testObjectName).getName());
      assertEquals(2, store.get(testObjectName).getValue());
    });
    assertEquals(1, store.size());
  }

  @Test
  public void testStream() {
    JacisStore<String, TestObject, byte[]> store = new JacisTestHelper().createTestStoreWithSerialization();
    store.getContainer().withLocalTx(() -> {
      store.update("1", new TestObject("A1", 1));
      store.update("2", new TestObject("A2", 2));
      store.update("3", new TestObject("B1", 3));
      store.update("4", new TestObject("B2", 4));
    });
    store.getContainer().withLocalTx(() -> {
      store.update("5", new TestObject("A3", 5));
      long res = store.stream().filter(o -> o.getName().startsWith("A")).mapToLong(o -> o.getValue()).sum();
      assertEquals(1 + 2 + 5, res);
    });
  }

  @Test
  public void testCollect() {
    JacisStore<String, TestObject, byte[]> store = new JacisTestHelper().createTestStoreWithSerialization();
    store.getContainer().withLocalTx(() -> {
      store.update("1", new TestObject("A1", 1));
      store.update("2", new TestObject("A2", 2));
      store.update("3", new TestObject("B1", 3));
      store.update("4", new TestObject("B2", 4));
    });
    store.getContainer().withLocalTx(() -> {
      store.update("5", new TestObject("A3", 5));
      AtomicInteger res = store.collect(new AtomicInteger(), (c, o) -> {
        if (o != null && o.getName().startsWith("A")) {
          c.incrementAndGet();
        }
      });
      assertEquals(3, res.get());
    });
  }

  @Test
  public void testTrackOriginalVersion() {
    JacisStore<String, TestObject, byte[]> store = new JacisTestHelper().createTestStoreWithSerialization();
    String testObjectName = "obj-1";
    TestObject testObject1 = new TestObject(testObjectName, 1);
    store.getContainer().withLocalTx(() -> {
      store.update(testObjectName, testObject1);
    });
    store.getContainer().withLocalTx(() -> {
      TestObject testObject2 = store.get(testObjectName);
      store.update(testObjectName, testObject2.setValue(2));
      assertEquals(testObject2, store.get(testObjectName));
      assertEquals(String.valueOf(testObject1), store.getObjectInfo(testObjectName).getOriginalTxViewValueString());
    });
  }

}
