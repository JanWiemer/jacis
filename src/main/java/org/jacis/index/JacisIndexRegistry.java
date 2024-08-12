package org.jacis.index;

import org.jacis.JacisApi;
import org.jacis.container.JacisTransactionHandle;
import org.jacis.exception.JacisUniqueIndexViolationException;
import org.jacis.plugin.JacisModificationListener;
import org.jacis.store.JacisStoreImpl;
import org.jacis.store.KeyValuePair;
import org.jacis.util.ConcurrentHashSet;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * The index registry stores all indices registered for the corresponding store.
 * The stored data contains the index definition (with the function to compute the index key for an object)
 * and the data maps for the indices.
 * For each index the data map contains the mapping from the index key to the primary key of the object in the store.
 * <p>
 * Note that only modifications notified to the store via the update method are tracked at the indices.
 *
 * @param <K>  Key type of the store entry
 * @param <TV> Type of the objects in the transaction view. This is the type visible from the outside.
 * @author Jan Wiemer
 */
@JacisApi
@SuppressWarnings({"unused", "UnusedReturnValue"}) // since this is an API of the library
public class JacisIndexRegistry<K, TV> implements JacisModificationListener<K, TV> {

  /** Reference to the store this index registry belongs to */
  private final JacisStoreImpl<K, TV, ?> store;

  /** Map containing the non-unique index definitions for each registered unique index */
  private final ConcurrentHashMap<String, JacisNonUniqueIndex<?, K, TV>> nonUniqueIndexDefinitionMap = new ConcurrentHashMap<>();
  /** The data maps for the non-unique indices. For each index the data map contains the mapping from the index key to the primary key of the object in the store. */
  private final Map<String, Map<Object, Set<K>>> nonUniqueIndexDataMap = new HashMap<>();

  /** Map containing the non-unique multi-index definitions for each registered unique index */
  private final ConcurrentHashMap<String, JacisNonUniqueMultiIndex<?, K, TV>> nonUniqueMultiIndexDefinitionMap = new ConcurrentHashMap<>();
  /** The data maps for the non-unique multi-indices. For each index the data map contains the mapping from the index key to the primary key of the object in the store. */
  private final Map<String, Map<Object, Set<K>>> nonUniqueMultiIndexDataMap = new HashMap<>();

  /** Map containing the unique index definitions for each registered unique index */
  private final ConcurrentHashMap<String, JacisUniqueIndex<?, K, TV>> uniqueIndexDefinitionMap = new ConcurrentHashMap<>();
  /** The data maps for the unique indices. For each index the data map contains the mapping from the index key to the primary keys of the objects in the store. */
  private final Map<String, Map<Object, K>> uniqueIndexDataMap = new HashMap<>();

  /**
   * The lock maps for the unique indices. For each index the lock map contains the mapping from the index key to the lock information for this index.
   * The lock information (see {@link IndexLock}) indicates if the index value is locked for another prepared transaction,
   * that means the other transaction will modify an object causing it to have this locked index value.
   * The index lock data contain the information of the primary key of the object that locks the index key and the transaction that will modify this object.
   */
  private final Map<String, Map<Object, IndexLock<K>>> uniqueIndexLockMap = new HashMap<>();

  public JacisIndexRegistry(JacisStoreImpl<K, TV, ?> store, boolean checkConsistencyAfterCommit) {
    this.store = store;
  }

  public int getNumberOfUniqueIndices() {
    return uniqueIndexDefinitionMap.size();
  }

  public int getNumberOfNonUniqueIndices() {
    return nonUniqueIndexDefinitionMap.size();
  }

  public int getNumberOfNonUniqueMultiIndices() {
    return nonUniqueMultiIndexDefinitionMap.size();
  }

  public void clearIndices() {
    store.executeAtomic(() -> {
      nonUniqueIndexDataMap.values().forEach(Map::clear);
      nonUniqueMultiIndexDataMap.values().forEach(Map::clear);
      uniqueIndexDataMap.values().forEach(Map::clear);
      uniqueIndexLockMap.clear();
    });
  }

  public boolean hasAnyRegisteredIndex() {
    boolean isEmpty = uniqueIndexDefinitionMap.isEmpty() //
        && nonUniqueIndexDefinitionMap.isEmpty() //
        && nonUniqueMultiIndexDefinitionMap.isEmpty();
    return !isEmpty;
  }

  public Collection<JacisNonUniqueIndex<?, K, TV>> getNonUniqueIndexDefinitions() {
    return nonUniqueIndexDefinitionMap.values();
  }

  public Collection<JacisNonUniqueMultiIndex<?, K, TV>> getNonUniqueMultiIndexDefinitions() {
    return nonUniqueMultiIndexDefinitionMap.values();
  }

  public Collection<JacisUniqueIndex<?, K, TV>> getUniqueIndexDefinitions() {
    return uniqueIndexDefinitionMap.values();
  }

  @SuppressWarnings("unchecked")
  public <IK> JacisNonUniqueIndex<IK, K, TV> getNonUniqueIndex(String indexName) {
    return (JacisNonUniqueIndex<IK, K, TV>) nonUniqueIndexDefinitionMap.get(indexName);
  }

  @SuppressWarnings("unchecked")
  public <IK> JacisNonUniqueMultiIndex<IK, K, TV> getNonUniqueMultiIndex(String indexName) {
    return (JacisNonUniqueMultiIndex<IK, K, TV>) nonUniqueMultiIndexDefinitionMap.get(indexName);
  }

  @SuppressWarnings("unchecked")
  public <IK> JacisUniqueIndex<IK, K, TV> getUniqueIndex(String indexName) {
    return (JacisUniqueIndex<IK, K, TV>) uniqueIndexDefinitionMap.get(indexName);
  }


  @SuppressWarnings("unchecked")
  public <IK> JacisNonUniqueIndex<IK, K, TV> createNonUniqueIndex(String indexName, Function<TV, IK> indexKeyFunction) {
    JacisNonUniqueIndex<IK, K, TV> idx = (JacisNonUniqueIndex<IK, K, TV>) nonUniqueIndexDefinitionMap.compute(indexName, (k, v) -> {
      if (v != null) {
        throw new IllegalStateException("An index with the name " + k + " is already registered at the store " + store);
      } else {
        return new JacisNonUniqueIndex<>(indexName, indexKeyFunction, this);
      }
    });
    initializeNonUniqueIndex(idx);
    return Objects.requireNonNull(idx);
  }

  @SuppressWarnings("unchecked")
  public <IK> JacisNonUniqueMultiIndex<IK, K, TV> createNonUniqueMultiIndex(String indexName, Function<TV, Set<IK>> indexKeyFunction) {
    JacisNonUniqueMultiIndex<IK, K, TV> idx = (JacisNonUniqueMultiIndex<IK, K, TV>) nonUniqueMultiIndexDefinitionMap.compute(indexName, (k, v) -> {
      if (v != null) {
        throw new IllegalStateException("An index with the name " + k + " is already registered at the store " + store);
      } else {
        return new JacisNonUniqueMultiIndex<>(indexName, indexKeyFunction, this);
      }
    });
    initializeNonUniqueMultiIndex(idx);
    return Objects.requireNonNull(idx);
  }

  @SuppressWarnings("unchecked")
  public <IK> JacisUniqueIndex<IK, K, TV> createUniqueIndex(String indexName, Function<TV, IK> indexKeyFunction) {
    JacisUniqueIndex<IK, K, TV> idx = (JacisUniqueIndex<IK, K, TV>) uniqueIndexDefinitionMap.compute(indexName, (k, v) -> {
      if (v != null) {
        throw new IllegalStateException("An index with the name " + k + " is already registered at the store " + store);
      } else {
        return new JacisUniqueIndex<>(indexName, indexKeyFunction, this);
      }
    });
    initializeUniqueIndex(idx);
    return Objects.requireNonNull(idx);
  }

  private <IK> void initializeNonUniqueMultiIndex(JacisNonUniqueMultiIndex<IK, K, TV> idx) {
    String indexName = idx.getIndexName();
    store.executeAtomic(() -> {
      checkRegisterModificationListener();
      nonUniqueMultiIndexDataMap.put(indexName, new ConcurrentHashMap<>());
      store.streamKeyValuePairsReadOnly(null).forEach(pair -> trackModificationAtNonUniqueMultiIndex(idx, pair.getKey(), null, pair.getVal()));
    });
  }

  private <IK> void initializeNonUniqueIndex(JacisNonUniqueIndex<IK, K, TV> idx) {
    String indexName = idx.getIndexName();
    store.executeAtomic(() -> {
      checkRegisterModificationListener();
      nonUniqueIndexDataMap.put(indexName, new ConcurrentHashMap<>());
      store.streamKeyValuePairsReadOnly(null).forEach(pair -> trackModificationAtNonUniqueIndex(idx, pair.getKey(), null, pair.getVal()));
    });
  }

  private <IK> void initializeUniqueIndex(JacisUniqueIndex<IK, K, TV> idx) {
    String indexName = idx.getIndexName();
    store.executeAtomic(() -> {
      checkRegisterModificationListener();
      uniqueIndexDataMap.put(indexName, new HashMap<>());
      store.streamKeyValuePairsReadOnly(null).forEach(pair -> trackModificationAtUniqueIndex(idx, pair.getKey(), null, pair.getVal()));
    });

  }

  private void checkRegisterModificationListener() {
    if (nonUniqueIndexDataMap.isEmpty() && nonUniqueMultiIndexDataMap.isEmpty() && uniqueIndexDataMap.isEmpty()) {
      store.registerModificationListener(this); // first index created -> register at store
    }
  }

  @Override
  public void onModification(K key, TV oldValue, TV newValue, JacisTransactionHandle tx) {
    for (JacisUniqueIndex<?, K, TV> idx : uniqueIndexDefinitionMap.values()) {
      trackModificationAtUniqueIndex(idx, key, oldValue, newValue);
    }
    for (JacisNonUniqueIndex<?, K, TV> idx : nonUniqueIndexDefinitionMap.values()) {
      trackModificationAtNonUniqueIndex(idx, key, oldValue, newValue);
    }
    for (JacisNonUniqueMultiIndex<?, K, TV> idx : nonUniqueMultiIndexDefinitionMap.values()) {
      trackModificationAtNonUniqueMultiIndex(idx, key, oldValue, newValue);
    }
  }

  private void trackModificationAtUniqueIndex(JacisUniqueIndex<?, K, TV> idx, K key, TV oldValue, TV newValue) {
    Function<TV, ?> indexKeyFunction = idx.getIndexKeyFunction();
    Map<Object, K> indexMap = uniqueIndexDataMap.get(idx.getIndexName());
    Object oldIndexKey = oldValue == null ? null : indexKeyFunction.apply(oldValue);
    Object newIndexKey = newValue == null ? null : indexKeyFunction.apply(newValue);
    if (oldIndexKey == null && newIndexKey == null) {
      return;
    } else if (oldIndexKey != null && oldIndexKey.equals(newIndexKey)) {
      return;
    }
    checkUniqueIndexProperty(idx, newIndexKey, key, false);
    if (oldIndexKey != null) {
      indexMap.remove(oldIndexKey, key);
    }
    if (newIndexKey != null) {
      indexMap.put(newIndexKey, key);
    }
  }

  private void trackModificationAtNonUniqueIndex(JacisNonUniqueIndex<?, K, TV> idx, K key, TV oldValue, TV newValue) {
    Function<TV, ?> indexKeyFunction = idx.getIndexKeyFunction();
    Map<Object, Set<K>> indexMap = nonUniqueIndexDataMap.get(idx.getIndexName());
    Object oldIndexKey = oldValue == null ? null : indexKeyFunction.apply(oldValue);
    Object newIndexKey = newValue == null ? null : indexKeyFunction.apply(newValue);
    if (oldIndexKey != null) {
      Set<K> primaryKeySet = indexMap.get(oldIndexKey);
      if (primaryKeySet != null) {
        primaryKeySet.remove(key);
      }
    }
    if (newIndexKey != null) {
      Set<K> primaryKeySet = indexMap.computeIfAbsent(newIndexKey, k -> new ConcurrentHashSet<>());
      primaryKeySet.add(key);
    }
  }

  private void trackModificationAtNonUniqueMultiIndex(JacisNonUniqueMultiIndex<?, K, TV> idx, K key, TV oldValue, TV newValue) {
    Function<TV, ?> indexKeyFunction = idx.getIndexKeyFunction();
    Map<Object, Set<K>> indexMap = nonUniqueMultiIndexDataMap.get(idx.getIndexName());
    Object oldIndexKeyObject = oldValue == null ? null : indexKeyFunction.apply(oldValue);
    Object newIndexKeyObject = newValue == null ? null : indexKeyFunction.apply(newValue);
    Set<?> oldIndexKeySet = oldIndexKeyObject == null ? Collections.emptySet() : (Set<?>) oldIndexKeyObject;
    Set<?> newIndexKeySet = newIndexKeyObject == null ? Collections.emptySet() : (Set<?>) newIndexKeyObject;
    for (Object oldIndexKey : oldIndexKeySet) {
      if (oldIndexKey != null) {
        Set<K> primaryKeySet = indexMap.get(oldIndexKey);
        if (primaryKeySet != null) {
          primaryKeySet.remove(key);
        }
      }
    }
    for (Object newIndexKey : newIndexKeySet) {
      if (newIndexKey != null) {
        Set<K> primaryKeySet = indexMap.computeIfAbsent(newIndexKey, k -> new ConcurrentHashSet<>());
        primaryKeySet.add(key);
      }
    }
  }

  @SuppressWarnings("unchecked")
  void checkUniqueIndexProperty(JacisUniqueIndex<?, K, TV> idx, Object newIndexKey, K primaryKey, boolean considerTx) {
    K oldPrimaryKeyForIdxKey;
    if (considerTx) {
      oldPrimaryKeyForIdxKey = ((JacisUniqueIndex<Object, K, TV>) idx).getPrimaryKey(newIndexKey);
    } else {
      Map<Object, K> indexMap = uniqueIndexDataMap.get(idx.getIndexName());
      oldPrimaryKeyForIdxKey = indexMap.get(newIndexKey);
    }
    if (oldPrimaryKeyForIdxKey != null && !oldPrimaryKeyForIdxKey.equals(primaryKey)) {
      String errorMsg = "Update object " + primaryKey +
          " would cause it to have index key " + newIndexKey +
          " for index " + idx.getIndexName() +
          " but there is already another object " + oldPrimaryKeyForIdxKey +
          " with this index key. Therefore the update would cause an unique index violation!";
      throw new JacisUniqueIndexViolationException(errorMsg);
    }
  }

  //----------------------------------------------------------------------------------------------------------
  //----- Access methods for NON-UNIQUE INDEX
  //----------------------------------------------------------------------------------------------------------

  <IK> Set<K> getFromNonUniqueIndexPrimaryKeys(JacisNonUniqueIndex<IK, K, TV> index, IK indexKey) {
    JacisIndexRegistryTxView<K, TV> regTxView = store.getIndexRegistryTransactionView(); // null if no TX
    String indexName = index.getIndexName();
    Map<Object, Set<K>> indexMap = nonUniqueIndexDataMap.get(indexName);
    Object ik = index.wrapIndexKey(indexKey);
    if (regTxView != null) {
      Set<K> add = regTxView.getPrimaryKeysAddedForNonUniqueIndex(indexName, ik);
      Set<K> del = regTxView.getPrimaryKeysDeletedForNonUniqueIndex(indexName, ik);
      if (!add.isEmpty() || !del.isEmpty()) {
        Set<K> res = new HashSet(indexMap.getOrDefault(ik, Collections.emptySet()));
        res.removeAll(del);
        res.addAll(add);
        return res;
      }
    }
    return indexMap.getOrDefault(ik, Collections.emptySet());
  }

  <IK> Set<K> multiGetFromNonUniqueIndexPrimaryKeys(JacisNonUniqueIndex<IK, K, TV> index, Collection<IK> indexKeys) {
    if (indexKeys == null || indexKeys.isEmpty()) {
      return Collections.emptySet();
    }
    Set<K> res = new HashSet<>();
    for (IK indexKey : indexKeys) {
      res.addAll(getFromNonUniqueIndexPrimaryKeys(index, indexKey));
    }
    return res;
  }

  <IK> Stream<TV> streamFromNonUniqueIndex(JacisNonUniqueIndex<IK, K, TV> index, IK indexKey) {
    Set<K> primaryKeys = getFromNonUniqueIndexPrimaryKeys(index, indexKey);
    return primaryKeys.stream().map(primaryKey -> store.get(primaryKey));
  }

  <IK> Collection<TV> getFromNonUniqueIndex(JacisNonUniqueIndex<IK, K, TV> index, IK indexKey) {
    Set<K> primaryKeys = getFromNonUniqueIndexPrimaryKeys(index, indexKey);
    Collection<TV> res = new ArrayList<>(primaryKeys.size());
    for (K primaryKey : primaryKeys) {
      res.add(store.get(primaryKey));
    }
    return res;
  }

  <IK> Collection<TV> multiGetFromNonUniqueIndex(JacisNonUniqueIndex<IK, K, TV> index, Collection<IK> indexKeys) {
    Set<K> primaryKeys = multiGetFromNonUniqueIndexPrimaryKeys(index, indexKeys);
    Collection<TV> res = new ArrayList<>(primaryKeys.size());
    for (K primaryKey : primaryKeys) {
      res.add(store.get(primaryKey));
    }
    return res;
  }

  <IK> Stream<TV> streamFromNonUniqueIndexReadOnly(JacisNonUniqueIndex<IK, K, TV> index, IK indexKey) {
    Set<K> primaryKeys = getFromNonUniqueIndexPrimaryKeys(index, indexKey);
    return primaryKeys.stream().map(primaryKey -> store.getReadOnly(primaryKey));
  }

  <IK> Collection<TV> getFromNonUniqueIndexReadOnly(JacisNonUniqueIndex<IK, K, TV> index, IK indexKey) {
    Set<K> primaryKeys = getFromNonUniqueIndexPrimaryKeys(index, indexKey);
    Collection<TV> res = new ArrayList<>(primaryKeys.size());
    for (K primaryKey : primaryKeys) {
      res.add(store.getReadOnly(primaryKey));
    }
    return res;
  }

  <IK> Collection<TV> multiGetFromNonUniqueIndexReadOnly(JacisNonUniqueIndex<IK, K, TV> index, Collection<IK> indexKeys) {
    Set<K> primaryKeys = multiGetFromNonUniqueIndexPrimaryKeys(index, indexKeys);
    Collection<TV> res = new ArrayList<>(primaryKeys.size());
    for (K primaryKey : primaryKeys) {
      res.add(store.getReadOnly(primaryKey));
    }
    return res;
  }

  //----------------------------------------------------------------------------------------------------------
  //----- Access methods for NON-UNIQUE MULTI-INDEX
  //----------------------------------------------------------------------------------------------------------

  <IK> Set<K> getFromNonUniqueMultiIndexPrimaryKeys(JacisNonUniqueMultiIndex<IK, K, TV> index, IK indexKey) {
    JacisIndexRegistryTxView<K, TV> regTxView = store.getIndexRegistryTransactionView(); // null if no TX
    String indexName = index.getIndexName();
    Map<Object, Set<K>> indexMap = nonUniqueMultiIndexDataMap.get(indexName);
    if (regTxView != null) {
      Set<K> add = regTxView.getPrimaryKeysAddedForNonUniqueIndex(indexName, indexKey);
      Set<K> del = regTxView.getPrimaryKeysDeletedForNonUniqueIndex(indexName, indexKey);
      if (!add.isEmpty() || !del.isEmpty()) {
        Set<K> res = new HashSet<>(indexMap.getOrDefault(indexKey, Collections.emptySet()));
        res.removeAll(del);
        res.addAll(add);
        return res;
      }
    }
    return indexMap.getOrDefault(indexKey, Collections.emptySet());
  }

  <IK> Set<K> multiGetFromNonUniqueMultiIndexPrimaryKeys(JacisNonUniqueMultiIndex<IK, K, TV> index, Collection<IK> indexKeys) {
    if (indexKeys == null || indexKeys.isEmpty()) {
      return Collections.emptySet();
    }
    Set<K> res = new HashSet<>();
    for (IK indexKey : indexKeys) {
      res.addAll(getFromNonUniqueMultiIndexPrimaryKeys(index, indexKey));
    }
    return res;
  }

  <IK> Stream<TV> streamFromNonUniqueMultiIndex(JacisNonUniqueMultiIndex<IK, K, TV> index, IK indexKey) {
    Set<K> primaryKeys = getFromNonUniqueMultiIndexPrimaryKeys(index, indexKey);
    return primaryKeys.stream().map(primaryKey -> store.get(primaryKey));
  }

  <IK> Collection<TV> getFromNonUniqueMultiIndex(JacisNonUniqueMultiIndex<IK, K, TV> index, IK indexKey) {
    Set<K> primaryKeys = getFromNonUniqueMultiIndexPrimaryKeys(index, indexKey);
    Collection<TV> res = new ArrayList<>(primaryKeys.size());
    for (K primaryKey : primaryKeys) {
      res.add(store.get(primaryKey));
    }
    return res;
  }

  <IK> Collection<TV> multiGetFromNonUniqueMultiIndex(JacisNonUniqueMultiIndex<IK, K, TV> index, Collection<IK> indexKeys) {
    Set<K> primaryKeys = multiGetFromNonUniqueMultiIndexPrimaryKeys(index, indexKeys);
    Collection<TV> res = new ArrayList<>(primaryKeys.size());
    for (K primaryKey : primaryKeys) {
      res.add(store.get(primaryKey));
    }
    return res;
  }

  <IK> Stream<TV> streamFromNonUniqueMultiIndexReadOnly(JacisNonUniqueMultiIndex<IK, K, TV> index, IK indexKey) {
    Set<K> primaryKeys = getFromNonUniqueMultiIndexPrimaryKeys(index, indexKey);
    return primaryKeys.stream().map(primaryKey -> store.getReadOnly(primaryKey));
  }

  <IK> Collection<TV> getFromNonUniqueMultiIndexReadOnly(JacisNonUniqueMultiIndex<IK, K, TV> index, IK indexKey) {
    Set<K> primaryKeys = getFromNonUniqueMultiIndexPrimaryKeys(index, indexKey);
    Collection<TV> res = new ArrayList<>(primaryKeys.size());
    for (K primaryKey : primaryKeys) {
      res.add(store.getReadOnly(primaryKey));
    }
    return res;
  }

  <IK> Collection<TV> multiGetFromNonUniqueMultiIndexReadOnly(JacisNonUniqueMultiIndex<IK, K, TV> index, Collection<IK> indexKeys) {
    Set<K> primaryKeys = multiGetFromNonUniqueMultiIndexPrimaryKeys(index, indexKeys);
    Collection<TV> res = new ArrayList<>(primaryKeys.size());
    for (K primaryKey : primaryKeys) {
      res.add(store.getReadOnly(primaryKey));
    }
    return res;
  }

  //----------------------------------------------------------------------------------------------------------
  //----- Access methods for UNIQUE INDEX
  //----------------------------------------------------------------------------------------------------------

  @java.lang.SuppressWarnings("java:S2789")
  <IK> K getFromUniqueIndexPrimaryKey(JacisUniqueIndex<IK, K, TV> index, IK indexKey) {
    JacisIndexRegistryTxView<K, TV> regTxView = store.getIndexRegistryTransactionView(); // null if no TX
    String indexName = index.getIndexName();
    Map<Object, K> indexMap = uniqueIndexDataMap.get(indexName);
    if (regTxView != null) {
      Optional<K> txLocalResult = regTxView.getPrimaryKeyFromUniqueIndex(indexName, indexKey);
      //noinspection OptionalAssignedToNull
      if (txLocalResult != null) {
        return txLocalResult.orElse(null);
      }
    }
    return indexMap.get(indexKey);
  }

  <IK> Set<K> multiGetFromUniqueIndexPrimaryKeys(JacisUniqueIndex<IK, K, TV> index, Collection<IK> indexKeys) {
    if (indexKeys == null || indexKeys.isEmpty()) {
      return Collections.emptySet();
    }
    Set<K> res = new HashSet<>();
    for (IK indexKey : indexKeys) {
      res.add(getFromUniqueIndexPrimaryKey(index, indexKey));
    }
    return res;
  }

  <IK> TV getFromUniqueIndex(JacisUniqueIndex<IK, K, TV> index, IK indexKey) {
    K primaryKey = getFromUniqueIndexPrimaryKey(index, indexKey);
    return primaryKey == null ? null : store.get(primaryKey);
  }

  <IK> Collection<TV> multiGetFromUniqueIndex(JacisUniqueIndex<IK, K, TV> index, Collection<IK> indexKeys) {
    Set<K> primaryKeys = multiGetFromUniqueIndexPrimaryKeys(index, indexKeys);
    Collection<TV> res = new ArrayList<>(primaryKeys.size());
    for (K primaryKey : primaryKeys) {
      res.add(store.get(primaryKey));
    }
    return res;
  }

  <IK> TV getFromUniqueIndexReadOnly(JacisUniqueIndex<IK, K, TV> index, IK indexKey) {
    K primaryKey = getFromUniqueIndexPrimaryKey(index, indexKey);
    return primaryKey == null ? null : store.getReadOnly(primaryKey);
  }

  <IK> Collection<TV> multiGetFromUniqueIndexReadOnly(JacisUniqueIndex<IK, K, TV> index, Collection<IK> indexKeys) {
    Set<K> primaryKeys = multiGetFromUniqueIndexPrimaryKeys(index, indexKeys);
    Collection<TV> res = new ArrayList<>(primaryKeys.size());
    for (K primaryKey : primaryKeys) {
      res.add(store.getReadOnly(primaryKey));
    }
    return res;
  }

  //----------------------------------------------------------------------------------------------------------
  //----- Lock methods for UNIQUE INDEX
  //----------------------------------------------------------------------------------------------------------

  public void lockUniqueIndexKeysForTx(JacisTransactionHandle txHandle) {
    if (!uniqueIndexDefinitionMap.isEmpty()) {
      String txId = txHandle.getTxId();
      for (JacisUniqueIndex<?, K, TV> idx : uniqueIndexDefinitionMap.values()) { // for all UNIQUE indices
        Function<TV, ?> indexKeyFunction = idx.getIndexKeyFunction();
        Map<Object, IndexLock<K>> locksForIndex = uniqueIndexLockMap.computeIfAbsent(idx.getIndexName(), k -> new HashMap<>());
        store.streamAllUpdated(null).forEach(pair -> {
          K primaryKey = pair.getKey();
          TV updatedVal = pair.getVal();
          Object newIndexKey = indexKeyFunction.apply(updatedVal);
          checkUniqueIndexProperty(idx, newIndexKey, primaryKey, false);
          IndexLock<K> existingLock = locksForIndex.get(newIndexKey);
          IndexLock<K> newLock = new IndexLock<>(primaryKey, txId);
          if (existingLock != null && !existingLock.equals(newLock)) {
            K lockingObjectPrimaryKey = existingLock.getKey();
            String lockingTx = existingLock.getVal();
            String errorMsg = "Preparing commit of updated object " + primaryKey +
                " in TX " + newIndexKey +
                " would cause it to have index key " + newIndexKey +
                " for index " + idx.getIndexName() +
                " but there is already another object " + lockingObjectPrimaryKey +
                " updated and prepared in TX " + lockingTx +
                " locking this index key. Therefore committing the update would cause an unique index violation!";
            throw new JacisUniqueIndexViolationException(errorMsg);
          }
          locksForIndex.put(newIndexKey, newLock);
        });
      }
    }
  }

  public void unlockUniqueIndexKeysForTx(JacisTransactionHandle txHandle) {
    if (!uniqueIndexDefinitionMap.isEmpty()) {
      String txId = txHandle.getTxId();
      for (JacisUniqueIndex<?, K, TV> idx : uniqueIndexDefinitionMap.values()) { // for all UNIQUE indices
        Function<TV, ?> indexKeyFunction = idx.getIndexKeyFunction();
        Map<Object, IndexLock<K>> locksForIndex = uniqueIndexLockMap.get(idx.getIndexName());
        if (locksForIndex == null) {
          continue;
        }
        store.streamAllUpdated(null).forEach(pair -> {
          TV updatedVal = pair.getVal();
          Object newIndexKey = indexKeyFunction.apply(updatedVal);
          IndexLock<K> existingLock = locksForIndex.get(newIndexKey);
          if (existingLock != null && existingLock.getVal().equals(txId)) {
            locksForIndex.remove(newIndexKey);
          }
        });
      }
    }
  }

  /**
   * Contains the lock information for a unique index key.
   * The object contains the locked primary key of the object locking the index key ({@link KeyValuePair#getKey()})
   * and the transaction ID of the object modifying this object ({@link KeyValuePair#getVal()}).
   *
   * @param <K> Type of the primary key.
   * @author Jan Wiemer
   */
  private final static class IndexLock<K> extends KeyValuePair<K, String> {

    private static final long serialVersionUID = 1L;

    public IndexLock(K first, String second) {
      super(first, second);
    }
  }

}
