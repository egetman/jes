package store.jesframework.common;

import java.beans.ConstructorProperties;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import lombok.EqualsAndHashCode;
import store.jesframework.Event;

/**
 * Common system event that indicates projector handling failure.
 */
@EqualsAndHashCode(callSuper = true)
public class ProjectionFailure extends Failure {

    /**
     * {@inheritDoc}.
     */
    @ConstructorProperties({"source", "byWhom", "offset", "cause"})
    public ProjectionFailure(@Nonnull Event source, @Nonnull String byWhom, long offset, @Nullable String cause) {
        super(source, byWhom, offset, cause);
    }

}
