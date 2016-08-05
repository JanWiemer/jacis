/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.cloning;

import org.jacis.container.JacisTransactionHandle;
import org.jacis.exception.ReadOnlyException;
import org.jacis.exception.ReadOnlyModeNotSupportedException;
import org.jacis.plugin.txadapter.local.JacisLocalTransaction;
import org.jacis.store.JacisStore;
import org.jacis.testhelper.JacisTestHelper;
import org.jacis.testhelper.TestObject;
import org.jacis.testhelper.TestObjectWithoutReadOnlyMode;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

@SuppressWarnings("CodeBlock2Expr")
public class JacisStoreWithCloningAdapterTest {

  private static final Logger log = LoggerFactory.getLogger(JacisStoreWithCloningAdapterTest.class);

  @Test
  public void testInsert() {
    JacisStore<String, TestObject, TestObject> store = new JacisTestHelper().createTestStoreWithCloning();
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
    JacisStore<String, TestObject, TestObject> store = new JacisTestHelper().createTestStoreWithCloning();
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
    JacisStore<String, TestObject, TestObject> store = new JacisTestHelper().createTestStoreWithCloning();
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
    JacisStore<String, TestObject, TestObject> store = new JacisTestHelper().createTestStoreWithCloning();
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
    JacisStore<String, TestObject, TestObject> store = new JacisTestHelper().createTestStoreWithCloning();
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
    JacisStore<String, TestObject, TestObject> store = new JacisTestHelper().createTestStoreWithCloning();
    store.getContainer().withLocalTx(() -> {
      store.update("1", new TestObject("A1", 1));
      store.update("2", new TestObject("A2", 2));
      store.update("3", new TestObject("B1", 3));
      store.update("4", new TestObject("B2", 4));
    });
    store.getContainer().withLocalTx(() -> {
      store.update("5", new TestObject("A3", 5));
      long res = store.stream().filter(o -> o.getName().startsWith("A")).mapToLong(TestObject::getValue).sum();
      assertEquals(1 + 2 + 5, res);
    });
  }

  @Test
  public void testCollect() {
    JacisStore<String, TestObject, TestObject> store = new JacisTestHelper().createTestStoreWithCloning();
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
    JacisStore<String, TestObject, TestObject> store = new JacisTestHelper().createTestStoreWithCloning();
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

  @Test(expected = ReadOnlyException.class)
  public void testGetReadView() {
    JacisStore<String, TestObject, TestObject> store = new JacisTestHelper().createTestStoreWithCloning();
    TestObject testObject = new TestObject("name", 1);
    // init store
    store.getContainer().withLocalTx(() -> {
      store.update(testObject.getName(), testObject);
    });
    // update with writable object
    store.getContainer().withLocalTx(() -> {
      TestObject obj = store.get(testObject.getName());
      obj.setValue(2);
      store.update(obj.getName(), obj);
    });
    // get as read only object
    store.getContainer().withLocalTx(() -> {
      TestObject obj = store.getReadOnly(testObject.getName());
      assertEquals(2, obj.getValue());
      obj.setValue(3); // exception expected here!
    });
  }

  @Test
  public void testGetReadViewIsolation() {
    JacisTestHelper testHelper = new JacisTestHelper();
    JacisStore<String, TestObject, TestObject> store = testHelper.createTestStoreWithCloning();
    TestObject testObject = new TestObject("name", 1);
    // init store
    store.getContainer().withLocalTx(() -> {
      store.update(testObject.getName(), testObject);
    });
    JacisLocalTransaction tx1 = store.getContainer().beginLocalTransaction("Testing Repeated Read");
    TestObject objReadOnly = store.getReadOnly("name");
    assertEquals(1, objReadOnly.getValue());
    JacisTransactionHandle tx1Handle = testHelper.suspendTx();
    // update in another comcurrent transaction
    store.getContainer().withLocalTx(() -> store.update("name", store.get("name").setValue(2)));
    testHelper.resumeTx(tx1Handle);
    assertEquals(1, objReadOnly.getValue()); // for the old read only view the value has not changed because writing back clones a new instance into the committed view store
    assertEquals(2, store.getReadOnly("name").getValue());
    tx1.commit();
  }

  @Test(expected = ReadOnlyModeNotSupportedException.class)
  public void testObjectWithoutReadOnlySupport() {
    JacisStore<String, TestObjectWithoutReadOnlyMode, TestObjectWithoutReadOnlyMode> store = new JacisTestHelper().createTestStoreWithCloningAndWithoutReadonlyMode();
    TestObjectWithoutReadOnlyMode testObject = new TestObjectWithoutReadOnlyMode("name", 1).setName("name");
    // init store
    store.getContainer().withLocalTx(() -> {
      store.update(testObject.getName(), testObject);
    });
    // update with writable object
    store.getContainer().withLocalTx(() -> {
      TestObjectWithoutReadOnlyMode obj = store.get(testObject.getName());
      obj.setValue(2);
      store.update(obj.getName(), obj);
    });
    // get as read only object
    store.getContainer().withLocalTx(() -> {
      TestObjectWithoutReadOnlyMode obj = store.getReadOnly(testObject.getName());
    });
  }

}
