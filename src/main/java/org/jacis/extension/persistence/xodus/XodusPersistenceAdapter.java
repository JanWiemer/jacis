package org.jacis.extension.persistence.xodus;

import org.jacis.store.KeyValuePair;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import jetbrains.exodus.ArrayByteIterable;
import jetbrains.exodus.ByteIterable;
import jetbrains.exodus.bindings.StringBinding;
import jetbrains.exodus.env.Environment;

/**
 * Implementation of the JACIS persistence adapter based on JetBrains Xodus
 * (see <a href="https://github.com/JetBrains/xodus">https://github.com/JetBrains/xodus</a>)
 * using JSON to serialize the JACIS objects.
 * 
 * @param <K> Key type of the store entry
 * @param <V> Value type of the store entry
 * 
 * @author Jan Wiemer
 */
public class XodusPersistenceAdapter<K, V> extends AbstractXodusPersistenceAdapter<K, V> {

  /** The JSON object mapper used to convert the objects and keys in the JACIS store to Strings. */
  private final ObjectMapper objectMapper;

  public XodusPersistenceAdapter(Environment env, ObjectMapper objectMapper, boolean traceLogging) {
    super(env, traceLogging);
    this.objectMapper = objectMapper;
  }

  public XodusPersistenceAdapter(Environment env, ObjectMapper objectMapper) {
    this(env, objectMapper, false);
  }

  @Override
  protected KeyValuePair<ByteIterable, ByteIterable> encode(K key, V value) {
    try {
      String keyString = objectMapper.writeValueAsString(key);
      String valString = value == null ? null : objectMapper.writeValueAsString(value);
      ArrayByteIterable xodusKey = StringBinding.stringToEntry(keyString);
      ArrayByteIterable xodusVal = valString == null ? null : StringBinding.stringToEntry(valString);
      return new KeyValuePair<>(xodusKey, xodusVal);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  protected KeyValuePair<K, V> decode(ByteIterable xodusKey, ByteIterable xodusValue) {
    try {
      String keyString = StringBinding.entryToString(xodusKey);
      String valString = StringBinding.entryToString(xodusValue);
      K key = (K) objectMapper.readValue(keyString, storeIdentifier.getKeyClass());
      V val = (V) objectMapper.readValue(valString, storeIdentifier.getValueClass());
      return new KeyValuePair<>(key, val);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

}
