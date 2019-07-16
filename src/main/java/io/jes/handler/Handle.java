package io.jes.handler;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation indicates that the method hanle some command in the system.
 */
@SuppressWarnings("WeakerAccess")
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Handle {

}
