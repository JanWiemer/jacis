package org.jacis.index;

import org.jacis.JacisApi;
import org.jacis.exception.JacisUniqueIndexViolationException;
import org.jacis.store.JacisStore;

import java.util.Collection;
import java.util.function.Function;

/**
 * Represents an index providing access to the values stored in the JACIS store by an index key.
 * The class represents a unique index, therefore for each index key only one value of the original store is returned.
 * Note that if a unique index is registered at a store a commit changing a value in a way that the unique index is violated
 * an {{@link JacisUniqueIndexViolationException}} is thrown.
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
public class JacisUniqueIndex<IK, K, TV> extends AbstractJacisIndex<IK, K, TV> {

  JacisUniqueIndex(String indexName, Function<TV, IK> indexKeyFunction, JacisIndexRegistry<K, TV> indexRegistry) {
    super(indexName, indexKeyFunction, indexRegistry);
  }

  /**
   * Returns the unique primary key for the passed index key.
   * The primary key is the key used to store the object in the store.
   *
   * @param indexKey The index key of the desired entry.
   * @return the unique primary key for the passed index key.
   */
  public K getPrimaryKey(IK indexKey) {
    return indexRegistry.getFromUniqueIndexPrimaryKey(this, indexKey);
  }

  /**
   * Returns the unique value for the passed index key.
   * For details see the {@link JacisStore#get(Object)} method.
   *
   * @param indexKey The index key of the desired entry.
   * @return the unique value for the passed index key.
   */
  public TV get(IK indexKey) {
    return indexRegistry.getFromUniqueIndex(this, indexKey);
  }

  /**
   * Returns the unique read only value for the passed index key.
   * For details see the {@link JacisStore#getReadOnly(Object)} method.
   *
   * @param indexKey The index key of the desired entry.
   * @return the unique read only value for the passed key.
   */
  public TV getReadOnly(IK indexKey) {
    return indexRegistry.getFromUniqueIndexReadOnly(this, indexKey);
  }

  /**
   * Returns the unique values for the passed index keys.
   * For details see the {@link JacisStore#get(Object)} method.
   *
   * @param indexKeys The index keys of the desired entrys.
   * @return the collection of unique values for the passed index keys.
   */
  public Collection<TV> multiGet(Collection<IK> indexKeys) {
    return indexRegistry.multiGetFromUniqueIndex(this, indexKeys);
  }

  /**
   * Returns the unique read only values for the passed index keys.
   * For details see the {@link JacisStore#getReadOnly(Object)} method.
   *
   * @param indexKeys The index keys of the desired entries.
   * @return the collection of unique read only values for the passed keys.
   */
  public Collection<TV> multiGetReadOnly(Collection<IK> indexKeys) {
    return indexRegistry.multiGetFromUniqueIndexReadOnly(this, indexKeys);
  }
}
