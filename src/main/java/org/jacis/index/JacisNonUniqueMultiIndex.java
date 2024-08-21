package org.jacis.index;

import org.jacis.JacisApi;
import org.jacis.store.JacisStore;

import java.util.Collection;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Represents an index providing access to the values stored in the JACIS store by an index key.
 * The class represents a non-unique index, therefore for each index one or more value of the original store may be returned.
 * Furthermore, the class represents a multi-index meaning that one value can have multiple index keys to access it.
 * <p>
 * Note that modifications inside the active transactions will be reflected by the index
 * if (and only if) the modification is notified to the store by the update method.
 *
 * @param <IK> Index key type for this unique index
 * @param <K>  Key type of the store entry
 * @param <TV> Type of the objects in the transaction view. This is the type visible from the outside.
 * @author Jan Wiemer
 */
@JacisApi
public class JacisNonUniqueMultiIndex<IK, K, TV> extends AbstractJacisMultiIndex<IK, K, TV> {

  JacisNonUniqueMultiIndex(String indexName, Function<TV, Set<IK>> indexKeyFunction, JacisIndexRegistry<K, TV> indexRegistry) {
    super(indexName, indexKeyFunction, indexRegistry);
  }

  /**
   * Returns a stream of the primary keys for the passed index key.
   * The primary keys are the keys used to store the object in the store.
   *
   * @param indexKey The index key of the desired entry.
   * @return a stream of the primary keys for the passed index key.
   */
  public Stream<K> streamPrimaryKeys(IK indexKey) {
    return indexRegistry.streamFromNonUniqueMultiIndexPrimaryKeys(this, indexKey);
  }

  /**
   * Returns the primary keys for the passed index key.
   * The primary keys are the keys used to store the object in the store.
   *
   * @param indexKey The index key of the desired entry.
   * @return the collection of primary keys for the passed index key.
   */
  public Set<K> getPrimaryKeys(IK indexKey) {
    return indexRegistry.getFromNonUniqueMultiIndexPrimaryKeys(this, indexKey);
  }

  /**
   * Returns a stream of the values for the passed index key.
   * For details see the {@link JacisStore#get(Object)} method.
   *
   * @param indexKey The index key of the desired entry.
   * @return a stream of the values for the passed index key.
   */
  public Stream<TV> stream(IK indexKey) {
    return indexRegistry.streamFromNonUniqueMultiIndex(this, indexKey);
  }

  /**
   * Returns the values for the passed index key.
   * For details see the {@link JacisStore#get(Object)} method.
   *
   * @param indexKey The index key of the desired entry.
   * @return the values for the passed index key.
   */
  public Collection<TV> get(IK indexKey) {
    return indexRegistry.getFromNonUniqueMultiIndex(this, indexKey);
  }

  /**
   * Returns a stream of the read only values for the passed index key.
   * For details see the {@link JacisStore#getReadOnly(Object)} method.
   *
   * @param indexKey The index key of the desired entry.
   * @return a stream of the read only values for the passed key.
   */
  public Stream<TV> streamReadOnly(IK indexKey) {
    return indexRegistry.streamFromNonUniqueMultiIndexReadOnly(this, indexKey);
  }

  /**
   * Returns the read only values for the passed index key.
   * For details see the {@link JacisStore#getReadOnly(Object)} method.
   *
   * @param indexKey The index key of the desired entry.
   * @return the read only values for the passed key.
   */
  public Collection<TV> getReadOnly(IK indexKey) {
    return indexRegistry.getFromNonUniqueMultiIndexReadOnly(this, indexKey);
  }


  /**
   * Returns a stream of the values for the passed index keys.
   * For details see the {@link JacisStore#get(Object)} method.
   *
   * @param indexKeys The index keys of the desired entries.
   * @return a stream of the values for the passed index keys.
   */
  public Stream<TV> stream(Collection<IK> indexKeys) {
    return indexRegistry.streamFromNonUniqueMultiIndex(this, indexKeys);
  }

  /**
   * Returns the values for the passed index keys.
   * For details see the {@link JacisStore#get(Object)} method.
   *
   * @param indexKeys The index keys of the desired entries.
   * @return the values for the passed index keys.
   */
  public Collection<TV> multiGet(Collection<IK> indexKeys) {
    return indexRegistry.multiGetFromNonUniqueMultiIndex(this, indexKeys);
  }

  /**
   * Returns a stream of the read only values for the passed index keys.
   * For details see the {@link JacisStore#getReadOnly(Object)} method.
   *
   * @param indexKeys The index keys of the desired entries.
   * @return a stream of the read only values for the passed keys.
   */
  public Stream<TV> streamReadOnly(Collection<IK> indexKeys) {
    return indexRegistry.streamFromNonUniqueMultiIndexReadOnly(this, indexKeys);
  }

  /**
   * Returns the read only values for the passed index keys.
   * For details see the {@link JacisStore#getReadOnly(Object)} method.
   *
   * @param indexKeys The index keys of the desired entries.
   * @return the read only values for the passed keys.
   */
  public Collection<TV> multiGetReadOnly(Collection<IK> indexKeys) {
    return indexRegistry.multiGetFromNonUniqueMultiIndexReadOnly(this, indexKeys);
  }

}
