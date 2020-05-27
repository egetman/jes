package store.jesframework.internal;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.persistence.EntityManagerFactory;
import javax.persistence.spi.PersistenceUnitInfo;
import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import org.h2.jdbcx.JdbcConnectionPool;
import org.h2.jdbcx.JdbcDataSource;
import org.hibernate.dialect.PostgreSQL95Dialect;
import org.hibernate.jpa.HibernatePersistenceProvider;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.containers.PostgreSQLContainer;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import store.jesframework.util.Pair;

import static com.mysql.cj.conf.PropertyDefinitions.DatabaseTerm.SCHEMA;
import static com.mysql.cj.conf.PropertyKey.allowMultiQueries;
import static com.mysql.cj.conf.PropertyKey.databaseTerm;
import static java.time.LocalDateTime.now;
import static java.time.ZoneOffset.UTC;
import static java.util.stream.Collectors.toList;
import static org.hibernate.cfg.AvailableSettings.AUTOCOMMIT;
import static org.hibernate.cfg.AvailableSettings.DIALECT;
import static org.hibernate.cfg.AvailableSettings.FORMAT_SQL;
import static org.hibernate.cfg.AvailableSettings.HBM2DDL_AUTO;
import static org.hibernate.cfg.AvailableSettings.SHOW_SQL;
import static org.hibernate.cfg.AvailableSettings.STATEMENT_BATCH_SIZE;
import static org.hibernate.cfg.AvailableSettings.USE_QUERY_CACHE;
import static org.hibernate.cfg.AvailableSettings.USE_STRUCTURED_CACHE;

@Slf4j
public final class FancyStuff {

    private static final int MAX_POOL_SIZE = 10;
    private static final int REDIS_EXPOSED_PORT = 6379;
    private static final String REDIS_URL_PATTERN = "redis://%s:%d";
    private static final String DEFAULT_TEST_USER = "jes_user";
    private static final String DEFAULT_TEST_PASSWORD = "jes_password";

    private FancyStuff() {}

    @Nonnull
    private static GenericContainer<?> newRedisContainer() {
        final GenericContainer<?> redisContainer =
                        new GenericContainer<>("redis:5.0")
                        .withExposedPorts(REDIS_EXPOSED_PORT);
        redisContainer.start();
        return redisContainer;
    }

    @Nonnull
    public static RedissonClient newRedissonClient() {
        final GenericContainer<?> redisContainer = newRedisContainer();
        final Config config = new Config();

        final String redisAddress = String.format(REDIS_URL_PATTERN, redisContainer.getContainerIpAddress(),
                redisContainer.getMappedPort(REDIS_EXPOSED_PORT));
        config.useSingleServer().setAddress(redisAddress);
        return Redisson.create(config);
    }

    @Nonnull
    private static PostgreSQLContainer<?> newPostgreSQLContainer(@Nonnull String dockerTag) {
        final PostgreSQLContainer<?> container = new PostgreSQLContainer<>(dockerTag)
                .withDatabaseName("jes")
                .withUsername(DEFAULT_TEST_USER)
                .withPassword(DEFAULT_TEST_PASSWORD);
        container.start();
        return container;
    }

    @Nonnull
    private static MySQLContainer<?> newMySQLContainer(@Nonnull String dockerTag, @Nonnull String schemaName) {
        final MySQLContainer<?> container = new MySQLContainer<>(dockerTag)
                .withDatabaseName(schemaName)
                .withUsername(DEFAULT_TEST_USER)
                .withPassword(DEFAULT_TEST_PASSWORD);
        container.start();
        return container;
    }

    @Nonnull
    private static OracleContainer newOracleContainer(@Nonnull String dockerTag) {
        final OracleContainer container = new OracleContainer(dockerTag)
                .withUsername("system")
                .withPassword("oracle")
                .withSharedMemorySize(2147483648L)
                .withEnv("ORACLE_ALLOW_REMOTE", "true")
                .withEnv("ORACLE_ENABLE_XDB", "true")
                .withInitScript("ddl/oracle-schema-setup.ddl");
        container.start();
        container.withUsername(DEFAULT_TEST_USER).withPassword(DEFAULT_TEST_PASSWORD);
        return container;
    }

    @Nonnull
    public static DataSource newH2DataSource() {
        final JdbcDataSource jdbcDataSource = new JdbcDataSource();
        jdbcDataSource.setUser(DEFAULT_TEST_USER);
        jdbcDataSource.setPasswordChars(DEFAULT_TEST_PASSWORD.toCharArray());
        jdbcDataSource.setUrl("jdbc:h2:mem:jes-" + UUID.randomUUID());
        return JdbcConnectionPool.create(jdbcDataSource);
    }

    @Nonnull
    public static DataSource newPostgresDataSource() {
        return newPostgresDataSource("public", "postgres:latest");
    }

    @Nonnull
    public static DataSource newPostgresDataSource(@Nonnull String schemaName) {
        return newPostgresDataSource(schemaName, "postgres:latest");
    }

    @Nonnull
    public static DataSource newPostgresDataSource(@Nonnull String schemaName, @Nonnull String dockerTag) {
        schemaName = schemaName + "_" + now().toInstant(UTC).toEpochMilli();
        final PostgreSQLContainer<?> container = newPostgreSQLContainer(dockerTag);
        final DataSource dataSource = from(container, schemaName, null);
        createSchema(dataSource, schemaName);
        return dataSource;
    }

    @Nonnull
    public static DataSource newMySqlDataSource(@Nonnull String schemaName) {
        return newMySqlDataSource(schemaName, "mysql:latest");
    }

    @Nonnull
    public static DataSource newMySqlDataSource(@Nonnull String schemaName, @Nonnull String dockerTag) {
        schemaName = schemaName + "_" + now().toInstant(UTC).toEpochMilli();
        final MySQLContainer<?> container = newMySQLContainer(dockerTag, schemaName);
        final Properties dataSourceProperties = new Properties();
        dataSourceProperties.put(databaseTerm, SCHEMA);
        dataSourceProperties.put(allowMultiQueries, true);
        final DataSource dataSource = from(container, schemaName, dataSourceProperties);
        createSchema(dataSource, schemaName);
        return dataSource;
    }

    @Nonnull
    public static DataSource newOracleDataSource() {
        return newOracleDataSource("oracleinanutshell/oracle-xe-11g");
    }

    @Nonnull
    public static DataSource newOracleDataSource(@Nonnull String dockerTag) {
        final OracleContainer container = newOracleContainer(dockerTag);
        return from(container, container.getUsername(), null);
    }

    private static DataSource from(@Nonnull JdbcDatabaseContainer<?> container, @Nonnull String schemaName,
                                   @Nullable Properties properties) {
        final HikariConfig config = new HikariConfig();

        config.setUsername(container.getUsername());
        config.setSchema(Objects.requireNonNull(schemaName, "Schema name must not be null"));
        config.setPassword(container.getPassword());
        config.setMaximumPoolSize(MAX_POOL_SIZE);
        config.setJdbcUrl(container.getJdbcUrl());
        config.setDriverClassName(container.getDriverClassName());
        if (properties != null) {
            config.setDataSourceProperties(properties);
        }
        final HikariDataSource dataSource = new HikariDataSource(config);
        log.debug("url: {}", config.getJdbcUrl());
        return dataSource;
    }

    @Nonnull
    public static Pair<DataSource, Collection<DataSource>> newPostgresClusterDataSource(int replicasCount) {
        final String schemaName =  "public_" + now().toInstant(UTC).toEpochMilli();
        final PostgreSQLClusterContainer container = new PostgreSQLClusterContainer(replicasCount);
        container.start();
        final PostgreSQLClusterContainer.PostgresCluster cluster = container.getClusterInfo();
        final DataSource master = from(cluster.getMaster(), schemaName, null);
        createSchema(master, schemaName);
        return Pair.of(master,
                cluster.getReplicas().stream().map(repl -> from(repl, schemaName, null)).collect(toList()));
    }

    /**
     * Connection will return non null schema name only if it exists for local containers run.
     *
     * @param schemaName name of schema to create.
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
        properties.put(SHOW_SQL, false);
        properties.put(FORMAT_SQL, false);
        properties.put(USE_STRUCTURED_CACHE, false);
        properties.put(STATEMENT_BATCH_SIZE, 20);
        properties.put(HBM2DDL_AUTO, "create");
        properties.put(AUTOCOMMIT, false);
        return new HibernatePersistenceProvider().createContainerEntityManagerFactory(unitInfo, properties);
    }

    @Nonnull
    private static PersistenceUnitInfo newUnit(Class<?> serializationType) {
        return new JesUnitInfo(newPostgresDataSource(), serializationType);
    }

    @Nonnull
    public static EntityManagerFactory newEntityManagerFactory(Class<?> serializationType) {
        final PersistenceUnitInfo unitInfo = newUnit(serializationType);
        return newEntityManagerFactory(unitInfo);
    }

}
