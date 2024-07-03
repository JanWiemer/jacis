package org.jacis.index;

import java.util.*;
import java.util.function.Function;

/**
 * Representing the transactional view on the indexes registered at the store for one transaction.
 * The transactional view stores the mapping of the index keys to the primary keys of the store
 * modified (due to modifications on the objects) inside the corresponding transaction.
 * Note that only modifications notified to the store via the update method are tracked at the indices.
 *
 * @param <K>  Key type of the store entry
 * @param <TV> Type of the objects in the transaction view. This is the type visible from the outside.
 * @author Jan Wiemer
 */
public class JacisIndexRegistryTxView<K, TV> {

  /** Reference to the index registry storing all indices registered for a store. */
  private final JacisIndexRegistry<K, TV> indexRegistry;
  /** Map containing the primary keys added for each index key inside the transaction for each non-unique index. */
  private final Map<String, Map<Object, Set<K>>> nonUniqueIndexAddMap = new HashMap<>();
  /** Map containing the primary keys removed for each index key inside the transaction for each non-unique index. */
  private final Map<String, Map<Object, Set<K>>> nonUniqueIndexDelMap = new HashMap<>();
  /** Map containing the unique index data for each unique index. */
  private final Map<String, Map<Object, Optional<K>>> uniqueIndexDataMap = new HashMap<>();

  public JacisIndexRegistryTxView(JacisIndexRegistry<K, TV> indexRegistry) {
    this.indexRegistry = indexRegistry;
  }

  public void onTxLocalUpdate(K key, TV oldValue, TV newValue) {
    for (JacisNonUniqueIndex<?, K, TV> idx : indexRegistry.getNonUniqueIndexDefinitions()) {
      Function<TV, ?> indexKeyFunction = idx.getIndexKeyFunction();
      Object oldIndexKey = oldValue == null ? null : indexKeyFunction.apply(oldValue);
      Object newIndexKey = newValue == null ? null : indexKeyFunction.apply(newValue);
      Map<Object, Set<K>> addMap = nonUniqueIndexAddMap.computeIfAbsent(idx.getIndexName(), k -> new HashMap<>());
      Map<Object, Set<K>> delMap = nonUniqueIndexDelMap.computeIfAbsent(idx.getIndexName(), k -> new HashMap<>());
      if (oldIndexKey == null && newIndexKey == null) {
        continue;
      } else if (oldIndexKey != null && oldIndexKey.equals(newIndexKey)) {
        continue;
      }
      if (oldIndexKey != null) {
        Set<K> oldPrimaryKeyAddSet = addMap.get(oldIndexKey);
        Set<K> oldPrimaryKeyDelSet = delMap.computeIfAbsent(oldIndexKey, k -> new HashSet<>());
        if (oldPrimaryKeyAddSet != null) {
          oldPrimaryKeyAddSet.remove(key);
        }
        oldPrimaryKeyDelSet.add(key);
      }
      if (newIndexKey != null) {
        Set<K> oldPrimaryKeyAddSet = addMap.computeIfAbsent(newIndexKey, k -> new HashSet<>());
        Set<K> oldPrimaryKeyDelSet = delMap.get(newIndexKey);
        oldPrimaryKeyAddSet.add(key);
        if (oldPrimaryKeyDelSet != null) {
          oldPrimaryKeyDelSet.remove(key);
        }
      }
    }
    for (JacisUniqueIndex<?, K, TV> idx : indexRegistry.getUniqueIndexDefinitions()) {
      Function<TV, ?> indexKeyFunction = idx.getIndexKeyFunction();
      Object oldIndexKey = oldValue == null ? null : indexKeyFunction.apply(oldValue);
      Object newIndexKey = newValue == null ? null : indexKeyFunction.apply(newValue);
      if (oldIndexKey == null && newIndexKey == null) {
        continue;
      } else if (oldIndexKey != null && oldIndexKey.equals(newIndexKey)) {
        continue;
      }
      Map<Object, Optional<K>> indexMap = uniqueIndexDataMap.computeIfAbsent(idx.getIndexName(), k -> new HashMap<>());
      indexRegistry.checkUniqueIndexProperty(idx, newIndexKey, key, true);
      if (oldIndexKey != null) {
        indexMap.put(oldIndexKey, Optional.empty());
      }
      if (newIndexKey != null) {
        indexMap.put(newIndexKey, Optional.of(key));
      }
    }
  }

  @java.lang.SuppressWarnings("java:S2789")
  public <IK> Optional<K> getPrimaryKeyFromUniqueIndex(String indexName, Object indexKey) {
    Map<Object, Optional<K>> indexMap = uniqueIndexDataMap.get(indexName);
    return indexMap == null ? null : indexMap.get(indexKey);
  }

  public <IK> Set<K> getPrimaryKeysAddedForNonUniqueIndex(String indexName, Object indexKey) {
    Map<Object, Set<K>> addMap = nonUniqueIndexAddMap.get(indexName);
    return addMap == null ? Collections.emptySet() : addMap.getOrDefault(indexKey, Collections.emptySet());
  }

  public <IK> Set<K> getPrimaryKeysDeletedForNonUniqueIndex(String indexName, Object indexKey) {
    Map<Object, Set<K>> delMap = nonUniqueIndexDelMap.get(indexName);
    return delMap == null ? Collections.emptySet() : delMap.getOrDefault(indexKey, Collections.emptySet());
  }

  public boolean isTrackingRequired() {
    return indexRegistry.hasAnyRegisteredIndex();
  }

}
