package org.jacis.index;

import org.jacis.JacisApi;

import java.util.Optional;
import java.util.function.Function;

/**
 * Abstract base class for Jacis Indices.
 *
 * @param <IK> Index key type for this unique index
 * @param <K>  Key type of the store entry
 * @param <TV> Type of the objects in the transaction view. This is the type visible from the outside.
 * @author Jan Wiemer
 */
@JacisApi
public abstract class AbstractJacisIndex<IK, K, TV> {

  /** Name of the index used to register it at the store. The index names have to be unique for one store. */
  protected final String indexName;
  /** Reference to the index registry storing all indices registered for a store. */
  protected final JacisIndexRegistry<K, TV> indexRegistry;
  /** Function defining how to compute the index key from the value stored in the store. */
  protected final Function<TV, IK> indexKeyFunction;
  /** Flag indicating if the values mapping to an index key of null should also be tracked. */
  protected final boolean trackNullIndexKeys;

  AbstractJacisIndex(String indexName, Function<TV, IK> indexKeyFunction, JacisIndexRegistry<K, TV> indexRegistry, boolean trackNullIndexKeys) {
    this.indexName = indexName;
    this.indexKeyFunction = indexKeyFunction;
    this.indexRegistry = indexRegistry;
    this.trackNullIndexKeys = trackNullIndexKeys;
  }

  AbstractJacisIndex(String indexName, Function<TV, IK> indexKeyFunction, JacisIndexRegistry<K, TV> indexRegistry) {
    this(indexName, indexKeyFunction, indexRegistry, true);
  }

  /** @return the name of the index (the name used to register the index during creation). */
  public String getIndexName() {
    return indexName;
  }

  protected boolean isTrackNullIndexKey() {
    return trackNullIndexKeys;
  }

  protected Object wrapIndexKey(Object key) {
    return key != null ? key : isTrackNullIndexKey() ? NULL_REPLACEMENT : null;
  }

  private static final Object NULL_REPLACEMENT = new Object() {
    @Override
    public String toString() {
      return "NULL_KEY";
    }
  };

  /** @return The function to compute an index key from the stored value. */
  @SuppressWarnings("unchecked")
  Function<TV, Object> getIndexKeyFunction() {
    if (isTrackNullIndexKey()) {
      return v -> Optional.<Object>ofNullable(indexKeyFunction.apply(v)).orElse(NULL_REPLACEMENT);
    } else {
      return (Function<TV, Object>) indexKeyFunction;
    }
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(" + indexName + ")";
  }

  @Override
  public int hashCode() {
    return indexName.hashCode();
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean equals(Object that) {
    if (that == null) {
      return false;
    }
    if (!this.getClass().equals(that.getClass())) {
      return false;
    }
    return this.indexName.equals(((AbstractJacisIndex<IK, K, TV>) that).indexName);
  }
}
