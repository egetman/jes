package store.jesframework.internal;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static java.time.temporal.ChronoUnit.SECONDS;

@Slf4j
@SuppressWarnings("SpellCheckingInspection")
public class PostgreSQLClusterContainer extends GenericContainer<PostgreSQLClusterContainer> {

    private static final String IMAGE = "sameersbn/postgresql";
    private static final String DEFAULT_TAG = "10-2";
    private static final String PG_USER = "postgres";
    private static final String PG_PASSWORD = "password";
    private static final String PG_REPL_USER = "repluser";
    private static final String PG_REPL_PASSWORD = "replpassword";
    private static final String PG_DB_NAME = "jes";

    private final PostgresCluster pgCluster;

    public PostgreSQLClusterContainer(int replicasCount) {
        if (replicasCount < 1) {
            throw new IllegalArgumentException("Replicas count must be greater than 0: " + replicasCount);
        }
        pgCluster = new PostgresCluster(createNewContainer("pg-master"));
        for (int i = 0; i < replicasCount; i++) {
            pgCluster.addReplica(createNewContainer("pg-slave-" + i));
        }
    }

    private PostgreSQLContainer<?> createNewContainer(String... aliases) {
        final PostgreSQLContainer<?> container = new PostgreSQLContainer<>(IMAGE + ":" + DEFAULT_TAG);
        container.withDatabaseName(PG_DB_NAME);
        container.withEnv("DB_NAME", PG_DB_NAME);
        container.withUsername(PG_USER);
        container.withEnv("DB_USER", PG_USER);
        container.withPassword(PG_PASSWORD);
        container.withEnv("DB_PASS", PG_PASSWORD);
        container.withEnv("PG_PASSWORD", PG_PASSWORD);
        container.withEnv("REPLICATION_USER", PG_REPL_USER);
        container.withEnv("REPLICATION_PASS", PG_REPL_PASSWORD);
        container.withEnv("PG_TRUST_LOCALNET", "true");
        container.withNetworkAliases(aliases);
        container.waitingFor(new LogMessageWaitStrategy()
                .withRegEx(".*database system is ready to accept( read only)? connections.*\\s")
                .withTimes(1)
                .withStartupTimeout(Duration.of(60, SECONDS)));
        container.withLogConsumer(new Slf4jLogConsumer(log));
        return container;
    }

    public PostgresCluster getClusterInfo() {
        return pgCluster;
    }

    @Override
    public void start() {
        pgCluster.start();
    }

    @Slf4j
    static class PostgresCluster implements AutoCloseable {

        private final Network network;
        private final PostgreSQLContainer<?> master;
        private final List<PostgreSQLContainer<?>> replicas = new ArrayList<>();

        PostgresCluster(@Nonnull PostgreSQLContainer<?> master) {
            this.network = Network.newNetwork();
            this.master = Objects.requireNonNull(master);
            this.master.withNetwork(network);
        }

        @SneakyThrows
        public void start() {
            master.start();
            log.info("PgMaster started");
            for (PostgreSQLContainer<?> replica : replicas) {
                replica.start();
            }
            log.info("{} PgReplica(s) started", replicas.size());
        }

        void addReplica(@Nonnull PostgreSQLContainer<?> replica) {
            replica.withNetwork(network);
            replica.withEnv("REPLICATION_MODE", "slave");
            replica.withEnv("REPLICATION_SSLMODE", "prefer");
            replica.withEnv("REPLICATION_HOST", "pg-master");
            replica.withEnv("REPLICATION_PORT", "5432");

            replicas.add(Objects.requireNonNull(replica));
        }

        PostgreSQLContainer<?> getMaster() {
            return master;
        }

        List<PostgreSQLContainer<?>> getReplicas() {
            return new ArrayList<>(replicas);
        }

        @Override
        public void close() {
            for (PostgreSQLContainer<?> replica : replicas) {
                replica.close();
            }
            master.close();
            network.close();
        }
    }
}
