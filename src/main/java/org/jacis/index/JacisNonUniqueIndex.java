package org.jacis.index;

import org.jacis.JacisApi;
import org.jacis.store.JacisStore;

import java.util.Collection;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

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
   * Returns a stream of the primary keys for the passed index key.
   * The primary keys are the keys used to store the object in the store.
   *
   * @param indexKey The index key of the desired entry.
   * @return a stream of the primary keys for the passed index key.
   */
  public Stream<K> streamPrimaryKeys(IK indexKey) {
    return indexRegistry.streamFromNonUniqueIndexPrimaryKeys(this, indexKey);
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
   * Returns a stream of the (writable) values for the passed index key.
   * For details see the {@link JacisStore#get(Object)} method.
   *
   * @param indexKey The index key of the desired entry.
   * @return a stream of the values for the passed index key.
   */
  public Stream<TV> stream(IK indexKey) {
    return indexRegistry.streamFromNonUniqueIndex(this, indexKey);
  }

  /**
   * Returns a stream of the (writable) values for the passed index key.
   * For details see the {@link JacisStore#get(Object)} method.
   * Before cloning the stored read-only instances into the transaction view
   * the passed filter is applied on the read-only instances.
   *
   * @param indexKey The index key of the desired entry.
   * @param filter   A filter applied to the read-only instances before cloning and returning them.
   * @return a stream of the values for the passed index key.
   */
  public Stream<TV> stream(IK indexKey, Predicate<TV> filter) {
    return indexRegistry.streamFromNonUniqueIndex(this, indexKey, filter);
  }

  /**
   * Returns the (writable) values for the passed index key.
   * For details see the {@link JacisStore#get(Object)} method.
   *
   * @param indexKey The index key of the desired entry.
   * @return the values for the passed index key.
   */
  public Collection<TV> get(IK indexKey) {
    return indexRegistry.getFromNonUniqueIndex(this, indexKey);
  }

  /**
   * Returns the (writable) values for the passed index key.
   * For details see the {@link JacisStore#get(Object)} method.
   * Before cloning the stored read-only instances into the transaction view
   * the passed filter is applied on the read-only instances.
   *
   * @param indexKey The index key of the desired entry.
   * @param filter   A filter applied to the read-only instances before cloning and returning them.
   * @return the values for the passed index key.
   */
  public Collection<TV> get(IK indexKey, Predicate<TV> filter) {
    return indexRegistry.getFromNonUniqueIndex(this, indexKey, filter);
  }

  /**
   * Returns a stream of the read only values for the passed index key.
   * For details see the {@link JacisStore#getReadOnly(Object)} method.
   *
   * @param indexKey The index key of the desired entry.
   * @return a stream of the read only values for the passed key.
   */
  public Stream<TV> streamReadOnly(IK indexKey) {
    return indexRegistry.streamFromNonUniqueIndexReadOnly(this, indexKey);
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


  /**
   * Returns a stream of the /writable) values for the passed index keys.
   * For details see the {@link JacisStore#get(Object)} method.
   *
   * @param indexKeys The index keys of the desired entries.
   * @return a stream of the values for the passed index keys.
   */
  public Stream<TV> stream(Collection<IK> indexKeys) {
    return indexRegistry.streamFromNonUniqueIndex(this, indexKeys);
  }

  /**
   * Returns a stream of the /writable) values for the passed index keys.
   * For details see the {@link JacisStore#get(Object)} method.
   * Before cloning the stored read-only instances into the transaction view
   * the passed filter is applied on the read-only instances.
   *
   * @param indexKeys The index keys of the desired entries.
   * @param filter    A filter applied to the read-only instances before cloning and returning them.
   * @return a stream of the values for the passed index keys.
   */
  public Stream<TV> stream(Collection<IK> indexKeys, Predicate<TV> filter) {
    return indexRegistry.streamFromNonUniqueIndex(this, indexKeys, filter);
  }

  /**
   * Returns the (writable) values for the passed index keys.
   * For details see the {@link JacisStore#get(Object)} method.
   *
   * @param indexKeys The index keys of the desired entries.
   * @return the values for the passed index keys.
   */
  public Collection<TV> multiGet(Collection<IK> indexKeys) {
    return indexRegistry.multiGetFromNonUniqueIndex(this, indexKeys);
  }

  /**
   * Returns the (writable) values for the passed index keys.
   * For details see the {@link JacisStore#get(Object)} method.
   * Before cloning the stored read-only instances into the transaction view
   * the passed filter is applied on the read-only instances.
   *
   * @param indexKeys The index keys of the desired entries.
   * @param filter    A filter applied to the read-only instances before cloning and returning them.
   * @return the values for the passed index keys.
   */
  public Collection<TV> multiGet(Collection<IK> indexKeys, Predicate<TV> filter) {
    return indexRegistry.multiGetFromNonUniqueIndex(this, indexKeys, filter);
  }


  /**
   * Returns a stream of the read only values for the passed index keys.
   * For details see the {@link JacisStore#getReadOnly(Object)} method.
   *
   * @param indexKeys The index keys of the desired entries.
   * @return a stream of the read only values for the passed keys.
   */
  public Stream<TV> streamReadOnly(Collection<IK> indexKeys) {
    return indexRegistry.streamFromNonUniqueIndexReadOnly(this, indexKeys);
  }

  /**
   * Returns the read only values for the passed index keys.
   * For details see the {@link JacisStore#getReadOnly(Object)} method.
   *
   * @param indexKeys The index keys of the desired entries.
   * @return the read only values for the passed keys.
   */
  public Collection<TV> multiGetReadOnly(Collection<IK> indexKeys) {
    return indexRegistry.multiGetFromNonUniqueIndexReadOnly(this, indexKeys);
  }

}
