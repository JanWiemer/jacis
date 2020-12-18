package org.jacis.persistence.microstream.typehandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import one.microstream.persistence.binary.internal.AbstractBinaryHandlerCustomValueVariableLength;
import one.microstream.persistence.binary.types.Binary;
import one.microstream.persistence.types.PersistenceLoadHandler;
import one.microstream.persistence.types.PersistenceStoreHandler;

public class TestObjectHandler extends AbstractBinaryHandlerCustomValueVariableLength<TestObject, byte[]> {

  private static final Logger log = LoggerFactory.getLogger(TestObjectHandler.class);

  public TestObjectHandler() {
    super(TestObject.class, CustomFields(bytes("binaryData")));
    log.info("instantiated: {} (-> {})", this.getClass(), this);
  }

  private byte[] instanceState(TestObject instance) {
    return (instance.getFirstName() + "|" + instance.getLastName() + "|" + instance.getAge()).getBytes();
  }

  @Override
  public void store(Binary data, TestObject instance, long objectId, PersistenceStoreHandler<Binary> handler) {
    log.info("store(.., {}, {}, ...) (data: {}, handler: {})", instance, objectId, data, handler);
    data.store_bytes(this.typeId(), objectId, instanceState(instance));
  }

  @Override
  public TestObject create(Binary data, PersistenceLoadHandler handler) {
    byte[] bytes = data.build_bytes();
    String stringData = new String(bytes);
    log.info("create(..., ...) -> {} (data: {}, handler: {})", stringData, data, handler);
    String[] tokens = stringData.split("\\|");
    TestObject res = new TestObject(tokens[0], tokens[1]).setAge(Integer.parseInt(tokens[2]));
    return res;
  }

  @Override
  public byte[] getValidationStateFromInstance(TestObject instance) {
    byte[] res = instanceState(instance);
    log.info("getValidationStateFromInstance({}) -> {}", instance, res);
    return res;
  }

  @Override
  public byte[] getValidationStateFromBinary(Binary data) {
    byte[] res = data.build_bytes();
    log.info("getValidationStateFromBinary({}) -> {}", data, res);
    return res;
  }

}
