package store.jesframework.common;

import java.beans.ConstructorProperties;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import lombok.EqualsAndHashCode;
import store.jesframework.Event;

/**
 * Common system event that indicates saga handling failure.
 */

@EqualsAndHashCode(callSuper = true)
public class SagaFailure extends Failure {

    /**
     * {@inheritDoc}.
     */
    @ConstructorProperties({"source", "byWhom", "offset", "cause"})
    public SagaFailure(@Nonnull Event source, @Nonnull String byWhom, long offset, @Nullable String cause) {
        super(source, byWhom, offset, cause);
    }

}
