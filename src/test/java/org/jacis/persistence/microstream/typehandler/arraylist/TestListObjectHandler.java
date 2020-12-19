package org.jacis.persistence.microstream.typehandler.arraylist;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import one.microstream.persistence.binary.internal.AbstractBinaryHandlerCustom;
import one.microstream.persistence.binary.types.Binary;
import one.microstream.persistence.types.PersistenceLoadHandler;
import one.microstream.persistence.types.PersistenceReferenceLoader;
import one.microstream.persistence.types.PersistenceStoreHandler;

public class TestListObjectHandler extends AbstractBinaryHandlerCustom<TestListObject> {

  private static final long BINARY_OFFSET_BASE = 0;
  private static final long BINARY_OFFSET_SIZE = BINARY_OFFSET_BASE;
  private static final long BINARY_OFFSET_ELEMENTS = BINARY_OFFSET_SIZE + Integer.BYTES;

  private static final Logger log = LoggerFactory.getLogger(TestListObjectHandler.class);

  public TestListObjectHandler() {
    super(TestListObject.class, SimpleArrayFields(CustomField(int.class, "size")));
    log.info("instantiated: {} (-> {})", this.getClass(), this);
  }

  @Override
  public void store(Binary data, TestListObject instance, long objectId, PersistenceStoreHandler<Binary> handler) {
    log.info("store(.., {}, {}, ...) (data: {}, handler: {})", instance, objectId, data, handler);
    int size = instance.getContent().size();
    long binarySize = BINARY_OFFSET_ELEMENTS - BINARY_OFFSET_BASE + Binary.referenceBinaryLength(size);
    data.storeEntityHeader(binarySize, this.typeId(), objectId);
    data.store_int(BINARY_OFFSET_SIZE, size);
    for (int i = 0; i < size; i++) {
//    for (Integer i : instance.getModified()) {
      String elem = instance.getContent().get(i);
      long elementObjectId = handler.apply(elem);
      log.info(" ... store modified object[{}] {} -> {}", i, elementObjectId, elem);
      data.store_long(BINARY_OFFSET_ELEMENTS + Binary.referenceBinaryLength(i), elementObjectId);
    }
    instance.getModified().clear();
  }

  @Override
  public TestListObject create(Binary data, PersistenceLoadHandler handler) {
    log.info("start create(..., ...) (data: {}, handler: {})", data, handler);
    int size = data.read_int(BINARY_OFFSET_SIZE);
    TestListObject res = new TestListObject(size);
    log.info("create(..., ...) -> {} ", res);
    return res;
  }

  @Override
  public void updateState(Binary data, TestListObject instance, PersistenceLoadHandler handler) {
    log.info("updateState(..., {},...) -> (data: {}, handler: {})", instance, data, handler);
    int size = data.read_int(BINARY_OFFSET_SIZE);
    for (int i = 0; i < size; i++) {
      long objectId = data.read_long(BINARY_OFFSET_ELEMENTS + Binary.referenceBinaryLength(i));
      log.info(" ... load object {}", objectId);
      instance.getContent().add((String) handler.lookupObject(objectId));
    }
  }

  @Override
  public void iterateLoadableReferences(Binary data, PersistenceReferenceLoader iterator) {
    log.info("iterateLoadableReferences(...,...) -> (data: {}, iterator: {})", data, iterator);
    int size = data.read_int(BINARY_OFFSET_SIZE);
    for (int i = 0; i < size; i++) {
      long objectId = data.read_long(BINARY_OFFSET_ELEMENTS + Binary.referenceBinaryLength(i));
      log.info(" ... register object {}", objectId);
      iterator.acceptObjectId(objectId);
    }
  }

  @Override
  public boolean hasPersistedReferences() {
    return true;
  }

  @Override
  public boolean hasVaryingPersistedLengthInstances() {
    return true;
  }

}
