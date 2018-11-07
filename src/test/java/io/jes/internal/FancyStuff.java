package io.jes.internal;

import java.sql.Connection;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.spi.PersistenceUnitInfo;
import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import org.hibernate.dialect.PostgreSQL95Dialect;
import org.hibernate.jpa.HibernatePersistenceProvider;
import org.testcontainers.containers.PostgreSQLContainer;

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

    private static final int MAX_POOL_SIZE = 30;

    private FancyStuff() {}

    @Nonnull
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

    @Nonnull
    public static DataSource newDataSource() {
        return newDataSource("public");
    }

    @Nonnull
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
     * Connection will return non null schema name only if it exists for local containers run.
     *
     * @param schemaName name of scheman to create.
     */
    @SneakyThrows
    private static void createSchema(@Nonnull DataSource dataSource, @Nonnull String schemaName) {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate(String.format("CREATE SCHEMA IF NOT EXISTS %s", schemaName));
        }
    }

    @Nonnull
    private static EntityManagerFactory newEntityManagerFactory(@Nonnull PersistenceUnitInfo unitInfo) {
        final Map<String, Object> properties = new HashMap<>();
        properties.put(DIALECT, PostgreSQL95Dialect.class);
        properties.put(USE_QUERY_CACHE, false);
        properties.put(SHOW_SQL, true);
        properties.put(FORMAT_SQL, true);
        properties.put(USE_STRUCTURED_CACHE, false);
        properties.put(STATEMENT_BATCH_SIZE, 20);
        properties.put(HBM2DDL_AUTO, "create");
        return new HibernatePersistenceProvider().createContainerEntityManagerFactory(unitInfo, properties);
    }

    @Nonnull
    private static PersistenceUnitInfo newUnit(Class<?> serializationType) {
        return new JesUnitInfo(newDataSource(), serializationType);
    }

    @Nonnull
    public static EntityManager newEntityManager(Class<?> serializationType) {
        final PersistenceUnitInfo unitInfo = newUnit(serializationType);

        return newEntityManagerFactory(unitInfo).createEntityManager();
    }

}
