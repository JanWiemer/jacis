package org.jacis.cloning;

import static org.junit.Assert.assertEquals;

import org.jacis.container.JacisContainer;
import org.jacis.store.JacisStore;
import org.jacis.testhelper.JacisTestHelper;
import org.jacis.testhelper.TestObject;
import org.jacis.testhelper.TrackedTestView;
import org.jacis.trackedviews.TrackedView;
import org.junit.Test;

public class JacisStoreWithCloningTrackedViewTest {

  @Test
  public void testTrackedView() {
    JacisTestHelper testHelper = new JacisTestHelper();
    JacisStore<String, TestObject, TestObject> store = testHelper.createTestStoreWithCloning();
    JacisContainer container = store.getContainer();
    container.withLocalTx(() -> {
      store.update("1", new TestObject("A1"));
      store.update("toDelete1", new TestObject("A1"));
    });
    TrackedView<TestObject> view = new TrackedTestView();
    store.getTrackedViewRegistry().registerTrackedView(view);
    container.withLocalTx(() -> {
      store.update("toDelete2", new TestObject("A1"));
      store.update("2", new TestObject("A2"));
      store.remove("toDelete2");
      store.update("3", new TestObject("B1"));
      store.update("4", new TestObject("B2"));
      store.remove("toDelete1");
    });
    container.withLocalTx(() -> {
      store.remove("toDelete2");
      store.update("toDelete3", new TestObject("A1"));
      store.remove("toDelete3");
      store.update("4", new TestObject("A4"));
      store.update("5", new TestObject("A3"));
      int viewVal = store.getTrackedViewRegistry().getView(TrackedTestView.class).getCount();
      assertEquals(5, viewVal);
    });
  }

}
