package io.jes.provider;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import javax.annotation.Nonnull;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.SharedCacheMode;
import javax.persistence.ValidationMode;
import javax.persistence.spi.ClassTransformer;
import javax.persistence.spi.PersistenceUnitInfo;
import javax.persistence.spi.PersistenceUnitTransactionType;
import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import org.hibernate.dialect.PostgreSQL95Dialect;
import org.hibernate.jpa.HibernatePersistenceProvider;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static org.hibernate.cfg.AvailableSettings.DIALECT;
import static org.hibernate.cfg.AvailableSettings.FORMAT_SQL;
import static org.hibernate.cfg.AvailableSettings.HBM2DDL_AUTO;
import static org.hibernate.cfg.AvailableSettings.SHOW_SQL;
import static org.hibernate.cfg.AvailableSettings.STATEMENT_BATCH_SIZE;
import static org.hibernate.cfg.AvailableSettings.USE_QUERY_CACHE;
import static org.hibernate.cfg.AvailableSettings.USE_STRUCTURED_CACHE;

@Slf4j
public final class FancyStuff {

    private static final int MAX_POOL_SIZE = 50;

    private FancyStuff() {}

    private static PostgreSQLContainer<?> newPostgreSQLContainer() {
        final String user = "user";
        final String password = "password";
        final PostgreSQLContainer container = new PostgreSQLContainer()
                .withDatabaseName("jes")
                .withUsername(user)
                .withPassword(password);
        container.start();
        return container;
    }

    static DataSource newDataSource() {
        return newDataSource("public");
    }

    public static DataSource newDataSource(@Nonnull String schemaName) {
        PostgreSQLContainer<?> container = newPostgreSQLContainer();
        final HikariConfig config = new HikariConfig();

        config.setUsername(container.getUsername());
        config.setSchema(Objects.requireNonNull(schemaName, "Schema name must not be null"));
        config.setPassword(container.getPassword());
        config.setMaximumPoolSize(MAX_POOL_SIZE);
        config.setJdbcUrl(container.getJdbcUrl());
        config.setDriverClassName(container.getDriverClassName());
        final HikariDataSource dataSource = new HikariDataSource(config);
        log.debug("url: {}", config.getJdbcUrl());
        createSchema(dataSource, schemaName);

        return dataSource;
    }

    /**
     * Connection will return non null schema name only if it exists.
     *
     * @param schemaName name of scheman to create.
     */
    @SneakyThrows
    private static void createSchema(@Nonnull DataSource dataSource, @Nonnull String schemaName) {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate(String.format("CREATE SCHEMA IF NOT EXISTS %s", schemaName));
        }
    }

    @SuppressWarnings("WeakerAccess")
    static EntityManagerFactory newEntityManagerFactory() {
        return new HibernatePersistenceProvider().createContainerEntityManagerFactory(persistenceUnitInfo(),
                ImmutableMap.<String, Object>builder()
                        .put(DIALECT, PostgreSQL95Dialect.class)
                        .put(USE_QUERY_CACHE, false)
                        .put(SHOW_SQL, true)
                        .put(FORMAT_SQL, true)
                        .put(USE_STRUCTURED_CACHE, false)
                        .put(STATEMENT_BATCH_SIZE, 20)
                        .put(HBM2DDL_AUTO, "create")
                        .build());
    }

    private static PersistenceUnitInfo persistenceUnitInfo() {
        return new PersistenceUnitInfo() {

            private final DataSource dataSource = newDataSource();

            @Override
            public String getPersistenceUnitName() {
                return "jes";
            }

            @Override
            public String getPersistenceProviderClassName() {
                return "org.hibernate.jpa.HibernatePersistenceProvider";
            }

            @Override
            public PersistenceUnitTransactionType getTransactionType() {
                return PersistenceUnitTransactionType.RESOURCE_LOCAL;
            }

            @Override
            public DataSource getJtaDataSource() {
                return dataSource;
            }

            @Override
            public DataSource getNonJtaDataSource() {
                return null;
            }

            @Override
            public List<String> getMappingFileNames() {
                return Collections.emptyList();
            }

            @Override
            public List<URL> getJarFileUrls() {
                try {
                    return Collections.list(this.getClass().getClassLoader().getResources(""));
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            @Override
            public URL getPersistenceUnitRootUrl() {
                return null;
            }

            @Override
            public List<String> getManagedClassNames() {
                return Collections.emptyList();
            }

            @Override
            public boolean excludeUnlistedClasses() {
                return false;
            }

            @Override
            public SharedCacheMode getSharedCacheMode() {
                return null;
            }

            @Override
            public ValidationMode getValidationMode() {
                return ValidationMode.AUTO;
            }

            @Override
            public Properties getProperties() {
                return new Properties();
            }

            @Override
            public String getPersistenceXMLSchemaVersion() {
                return null;
            }

            @Override
            public ClassLoader getClassLoader() {
                return null;
            }

            @Override
            public void addTransformer(ClassTransformer transformer) {

            }

            @Override
            public ClassLoader getNewTempClassLoader() {
                return null;
            }
        };
    }

    static EntityManager newEntityManager() {
        return newEntityManagerFactory().createEntityManager();
    }

}
