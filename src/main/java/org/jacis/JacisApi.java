package org.jacis;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * The public API of the classes annotated with this annotation belong the JACIS API.
 * 
 * @author Jan Wiemer
 */
@Documented
@Retention(RetentionPolicy.CLASS)
@Target(ElementType.TYPE)
public @interface JacisApi {
  // empty
}
