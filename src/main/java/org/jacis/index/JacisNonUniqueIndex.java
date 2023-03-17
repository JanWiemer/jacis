package org.jacis.index;

import java.util.Collection;
import java.util.Set;
import java.util.function.Function;

import org.jacis.JacisApi;
import org.jacis.store.JacisStore;

/**
 * Represents an index providing access to the values stored in the JACIS store by an index key.
 * The class represents a non-unique index, therefore for each index one or more value of the original store may be returned.
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
public class JacisNonUniqueIndex<IK, K, TV> extends AbstractJacisIndex<IK, K, TV> {

  JacisNonUniqueIndex(String indexName, Function<TV, IK> indexKeyFunction, JacisIndexRegistry<K, TV> indexRegistry) {
    super(indexName, indexKeyFunction, indexRegistry);
  }

  /**
   * Returns the primary keys for the passed index key.
   * The primary keys are the keys used to store the object in the store.
   *
   * @param indexKey The index key of the desired entry.
   * @return the collection of primary keys for the passed index key.
   */
  public Set<K> getPrimaryKeys(IK indexKey) {
    return indexRegistry.getFromNonUniqueIndexPrimaryKeys(this, indexKey);
  }

  /**
   * Returns the values for the passed index key.
   * For details see the {@link JacisStore#get(Object)} method.
   *
   * @param indexKey The index key of the desired entry.
   * @return the unique value for the passed index key.
   */
  public Collection<TV> get(IK indexKey) {
    return indexRegistry.getFromNonUniqueIndex(this, indexKey);
  }

  /**
   * Returns the read only values for the passed index key.
   * For details see the {@link JacisStore#getReadOnly(Object)} method.
   *
   * @param indexKey The index key of the desired entry.
   * @return the read only values for the passed key.
   */
  public Collection<TV> getReadOnly(IK indexKey) {
    return indexRegistry.getFromNonUniqueIndexReadOnly(this, indexKey);
  }

}
