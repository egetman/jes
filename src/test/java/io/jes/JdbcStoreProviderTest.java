package io.jes;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import org.junit.jupiter.api.Test;
import org.postgresql.Driver;
import org.testcontainers.containers.PostgreSQLContainer;

import io.jes.provider.StoreProvider;
import io.jes.provider.JdbcStoreProvider;
import io.jes.provider.jdbc.DataSourceType;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class JdbcStoreProviderTest {

    @Test
    void shouldReadOwnWrites() {
        StoreProvider provider = new JdbcStoreProvider(createDataSource(), DataSourceType.POSTGRESQL);
        provider.write(new SampleEvent("FOO"));
        provider.write(new SampleEvent("BAR"));
        provider.write(new SampleEvent("BAZ"));


        provider.readFrom(0).forEach(event -> log.info("{}", event));
    }

    private DataSource createDataSource() {
        final String user = "csi";
        final String password = "csi";
        final PostgreSQLContainer container = new PostgreSQLContainer()
                .withDatabaseName("jes")
                .withUsername(user)
                .withPassword(password);
        container.start();

        final HikariConfig config = new HikariConfig();

        config.setAutoCommit(false);
        config.setUsername(user);
        config.setPassword(password);
        config.setMaximumPoolSize(10);
        config.setPoolName("Hello Moto");
        //config.setJdbcUrl(container.getJdbcUrl());
        //config.setDriverClassName(container.getDriverClassName());
        config.setJdbcUrl("jdbc:postgresql://192.168.14.202:5432/csi");
        config.setDriverClassName(Driver.class.getName());
        final HikariDataSource dataSource = new HikariDataSource(config);
        log.debug("url: {}, driver: {}, user: {}, password: {}\n", config.getJdbcUrl(), user, password);

        return dataSource;
    }
}
