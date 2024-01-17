package org.jacis.index;

import org.jacis.JacisApi;

import java.util.Set;
import java.util.function.Function;

/**
 * Abstract base class for Jacis Indices where one value object can have multiple index keys.
 *
 * @param <IK> Index key type for this unique index
 * @param <K>  Key type of the store entry
 * @param <TV> Type of the objects in the transaction view. This is the type visible from the outside.
 * @author Jan Wiemer
 */
@JacisApi
public abstract class AbstractJacisMultiIndex<IK, K, TV> {

  /** Name of the index used to register it at the store. The index names have to be unique for one store. */
  protected final String indexName;
  /** Reference to the index registry storing all indices registered for a store. */
  protected final JacisIndexRegistry<K, TV> indexRegistry;
  /** Function defining how to compute the set of index keys from the value stored in the store. */
  protected final Function<TV, Set<IK>> indexKeyFunction;

  AbstractJacisMultiIndex(String indexName, Function<TV, Set<IK>> indexKeyFunction, JacisIndexRegistry<K, TV> indexRegistry) {
    this.indexName = indexName;
    this.indexKeyFunction = indexKeyFunction;
    this.indexRegistry = indexRegistry;
  }

  /** @return the name of the index (the name used to register the index during creation). */
  public String getIndexName() {
    return indexName;
  }

  /** @return The function to compute the set of the index keys from the stored value. */
  Function<TV, Set<IK>> getIndexKeyFunction() {
    return indexKeyFunction;
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
    return this.indexName.equals(((AbstractJacisMultiIndex<IK, K, TV>) that).indexName);
  }
}
