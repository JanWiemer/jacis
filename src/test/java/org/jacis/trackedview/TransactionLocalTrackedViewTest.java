/*
 * Copyright (c) 2020. Jan Wiemer
 */

package org.jacis.trackedview;

import static org.junit.Assert.assertEquals;

import org.jacis.container.JacisContainer;
import org.jacis.store.JacisStore;
import org.jacis.testhelper.JacisTestHelper;
import org.jacis.testhelper.TestObject;
import org.jacis.testhelper.TrackedTestView;
import org.jacis.trackedviews.TrackedView;
import org.junit.Test;

public class TransactionLocalTrackedViewTest {

  @Test
  public void testModificationTrackingInTransaction() {
    JacisTestHelper testHelper = new JacisTestHelper();
    JacisStore<String, TestObject> store = testHelper.createTestStoreWithCloning();
    JacisContainer container = store.getContainer();
    container.withLocalTx(() -> {
      store.update("1", new TestObject("A1").setValue(5));
    });
    TrackedView<TestObject> view = new TrackedTestView();
    store.getTrackedViewRegistry().registerTrackedView(view);
    assertEquals(5, store.getTrackedViewRegistry().getView(TrackedTestView.class).getSum());

    container.withLocalTx(() -> {
      TestObject o = store.get("1");
      o.setValue(3);
      store.update("1", o);
      assertEquals(3, store.getTrackedViewRegistry().getView(TrackedTestView.class).getSum());
      o.setValue(4);
      store.update("1", o);
      assertEquals(4, store.getTrackedViewRegistry().getView(TrackedTestView.class).getSum());
    });
    assertEquals(4, store.getTrackedViewRegistry().getView(TrackedTestView.class).getSum());
  }
}
