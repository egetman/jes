package store.jesframework.util;

import java.io.InputStream;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;

import store.jesframework.ex.PropertyNotFoundException;
import lombok.SneakyThrows;

public final class PropsReader {

    private static final Map<String, String> CACHE = new ConcurrentHashMap<>();

    private PropsReader() {
    }

    /**
     * Method used to read {@literal JES} properties.
     * First of all, it will cache all discovered property values and return it, if it found in the cache.
     * If property not found in the cache it will be searched in system properties.
     * If property not found through system properties it will search the property in environment properties.
     * If property not found through system properties it will read {@literal jes.properties} property file.
     * If property not found once again, {@link PropertyNotFoundException} will be thrown.
     *
     * @param propertyName name of the property to retrieve.
     * @return property value.
     * @throws PropertyNotFoundException if property not found by specified {@literal propertyName}.
     * @throws NullPointerException      if {@literal propertyName} is null.
     */
    @SneakyThrows
    public static String getProperty(@Nonnull String propertyName) {
        final String cached = CACHE.get(Objects.requireNonNull(propertyName, "Property name must not be null"));
        if (cached != null) {
            return cached;
        }

        final String systemProperty = System.getProperty(propertyName);
        if (systemProperty != null) {
            CACHE.put(propertyName, systemProperty);
            return systemProperty;
        }

        final String envProperty = System.getenv(propertyName);
        if (envProperty != null) {
            CACHE.put(propertyName, envProperty);
            return envProperty;
        }

        final ClassLoader loader = Thread.currentThread().getContextClassLoader();

        try (final InputStream inputStream = loader.getResourceAsStream("jes.properties")) {
            final Properties properties = new Properties();
            if (inputStream != null) {
                properties.load(inputStream);
            }
            final String property = properties.getProperty(propertyName);
            if (property != null) {
                CACHE.put(propertyName, property);
                return property;
            }
        }
        throw new PropertyNotFoundException(propertyName);
    }
}
