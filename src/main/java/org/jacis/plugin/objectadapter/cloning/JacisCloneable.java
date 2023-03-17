/*
 * Copyright (c) 2016. Jan Wiemer
 */

package org.jacis.plugin.objectadapter.cloning;

import org.jacis.JacisApi;

/**
 * This interface should be implemented by an object that should be cloned by the
 * {@link JacisCloningObjectAdapter} by simply calling the {@link #clone()} method.
 * <p>
 * Note that an implementation of the clone method usually overwrites the {@link Object#clone()} method.
 * This means a clone can be initialized by calling 'super.clone()' with the semantic that all primitive members
 * and all references are already cloned by the super method. How deep referred objects (specially referred
 * container objects like collections) should be cloned has to be carefully designed.
 * The design of the stored object together with the implementation of the clone method has to guarantee
 * that no modification of the cloned object itself or referred (reachable) objects can modify the original
 * object or its referred (reachable) objects. That means an object that is suitable to be a JACIS cloneable object
 * must only contain properties that are:
 * <p>
 * * primitive types, and therefore immutable;
 * * immutable types;
 * * other JACIS cloneable objects that are deeply cloned with the original object
 * * collections of other JACIS cloneable objects that are deeply cloned with the original object (the collection itself has to be cloned as well)
 * <p>
 * It is also possible to simply overwrite the {@link Object#clone()} method without implementing this interface.
 * However, in this case the {@link JacisCloningObjectAdapter} has to call the clone method by reflection.
 *
 * @param <OT> The type of the object implementing this interface.
 * @author Jan Wiemer
 */
@JacisApi
public interface JacisCloneable<OT> extends Cloneable {

  /** @return A clone of this object */
  OT clone();

}
