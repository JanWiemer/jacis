package org.jacis;

import static org.junit.Assert.assertNotNull;

import org.jacis.container.JacisContainer;
import org.jacis.container.JacisObjectTypeSpec;
import org.jacis.plugin.objectadapter.cloning.JacisCloningObjectAdapter;
import org.jacis.store.JacisStore;
import org.jacis.testhelper.TestObject;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JacisContainerTest {

  private static final Logger log = LoggerFactory.getLogger(JacisContainerTest.class);

  @Test
  public void testCreateContainer() {
    JacisContainer container = new JacisContainer();
    assertNotNull(container);
    log.info("The JacisContainer: {}", container);
  }

  @Test
  public void testRegisterStore() {
    JacisContainer container = new JacisContainer();
    JacisObjectTypeSpec<String, TestObject, TestObject> objectTypeSpec = new JacisObjectTypeSpec<>(String.class, TestObject.class, new JacisCloningObjectAdapter<TestObject>());
    container.createStore(objectTypeSpec);
    JacisStore<String, TestObject, TestObject> store = container.getStore(String.class, TestObject.class);
    assertNotNull(store);
  }

}
