/*
 * Copyright (c) 2017. Jan Wiemer
 */

package org.jacis.modificationlistener;

import org.jacis.container.JacisTransactionHandle;
import org.jacis.exception.JacisModificationVetoException;
import org.jacis.plugin.JacisModificationListener;
import org.jacis.store.JacisStore;
import org.jacis.testhelper.JacisTestHelper;
import org.jacis.testhelper.TestObject;
import org.junit.Test;

import static org.junit.Assert.*;

@SuppressWarnings("CodeBlock2Expr")
public class JacisModificationListenerTest {

  static class TestModificationListener implements JacisModificationListener<String, TestObject> {
    @Override
    public void onAdjustBeforePrepare(String key, TestObject oldValue, TestObject valueToCommit, JacisTransactionHandle tx) {
      if (oldValue != null) {
        assertTrue(oldValue.isReadOnly());
      }
      assertFalse(valueToCommit.isReadOnly());
      if (valueToCommit != null) {
        if ("A".equals(valueToCommit.getStrValue())) {
          valueToCommit.setValue(oldValue.getValue() + 1);
        }
        if (oldValue != null && "A".equals(oldValue.getStrValue())) {
          valueToCommit.setValue(oldValue.getValue() - 1);
        }
      }
    }

    @Override
    public void onPrepareModification(String key, TestObject oldValue, TestObject newValue, JacisTransactionHandle tx) throws JacisModificationVetoException {
      if (oldValue != null) {
        assertTrue(oldValue.isReadOnly());
      }
      if (newValue != null) {
        assertTrue(newValue.isReadOnly());
      }
    }

    @Override
    public void onModification(String key, TestObject oldValue, TestObject newValue, JacisTransactionHandle tx) {
      if (oldValue != null) {
        assertTrue(oldValue.isReadOnly());
      }
      if (newValue != null) {
        assertTrue(newValue.isReadOnly());
      }
      // do nothing
    }
  }

  @Test
  public void testGetCommittedVersion() {
    String objName = "OBJ";
    JacisTestHelper testHelper = new JacisTestHelper();
    JacisStore<String, TestObject> store = testHelper.createTestStoreWithCloning();
    store.registerModificationListener(new TestModificationListener());
    store.getContainer().withLocalTx(() -> store.update(objName, new TestObject(objName, 0)));
    assertEquals(0, store.getCommittedValue(objName).getValue());
    // --------------------
    store.getContainer().withLocalTx(() -> store.update(objName, new TestObject(objName, 0).setStrValue("A")));
    assertEquals(1, store.getCommittedValue(objName).getValue());
    // --------------------
    store.getContainer().withLocalTx(() -> store.update(objName, new TestObject(objName, 0).setStrValue("B")));
    assertEquals(0, store.getCommittedValue(objName).getValue());
    // --------------------
  }


}
