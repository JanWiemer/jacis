/*
 * Copyright (c) 2017. Jan Wiemer
 */

package org.jacis.modificationlistener;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.jacis.container.JacisContainer;
import org.jacis.container.JacisTransactionHandle;
import org.jacis.exception.JacisModificationVetoException;
import org.jacis.exception.JacisStaleObjectException;
import org.jacis.plugin.JacisModificationListener;
import org.jacis.plugin.txadapter.local.JacisLocalTransaction;
import org.jacis.store.JacisStore;
import org.jacis.store.JacisTransactionInfo;
import org.jacis.testhelper.JacisTestHelper;
import org.jacis.testhelper.TestObject;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("CodeBlock2Expr")
public class JacisModificationListenerTest {

  private static final Logger log = LoggerFactory.getLogger(JacisModificationListenerTest.class);

  static class TestModificationListener implements JacisModificationListener<String,TestObject> {

    @Override
    public void onPrepareModification(String key, TestObject oldValue, TestObject newValue, JacisTransactionHandle tx) throws JacisModificationVetoException {
      if(newValue!=null) {
        if( "A".equals(newValue.getStrValue())) {
          newValue.setValue(oldValue.getValue()+1);
        } 
        if(oldValue!=null && "A".equals(oldValue.getStrValue())) {
          newValue.setValue(oldValue.getValue()-1);
        }
      }
    }
    @Override
    public void onModification(String key, TestObject oldValue, TestObject newValue, JacisTransactionHandle tx) {
      // do nothing
    } 
  }
  
  @Test
  public void testGetCommittedVersion() {
    String objName = "OBJ";
    JacisTestHelper testHelper = new JacisTestHelper();
    JacisStore<String, TestObject> store = testHelper.createTestStoreWithCloning();
    store.registerModificationListener(new TestModificationListener());
    store.getContainer().withLocalTx(()->store.update(objName, new TestObject(objName, 0)));
    assertEquals(0, store.getCommittedValue(objName).getValue());
    // --------------------
    store.getContainer().withLocalTx(()->store.update(objName, new TestObject(objName, 0).setStrValue("A")));
    assertEquals(1, store.getCommittedValue(objName).getValue());
    // --------------------
    store.getContainer().withLocalTx(()->store.update(objName, new TestObject(objName, 0).setStrValue("B")));
    assertEquals(0, store.getCommittedValue(objName).getValue());
    // --------------------
  }

 
}
