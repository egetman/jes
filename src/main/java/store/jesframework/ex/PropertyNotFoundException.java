package store.jesframework.ex;

import javax.annotation.Nonnull;

public class PropertyNotFoundException extends RuntimeException {

    public PropertyNotFoundException(@Nonnull String propertyName) {
        super("Can't find specified property: " + propertyName + " in system properties and jes.properties file");
    }
}
