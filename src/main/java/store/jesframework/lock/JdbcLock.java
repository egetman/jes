package store.jesframework.lock;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.sql.DataSource;

import store.jesframework.ex.BrokenStoreException;
import store.jesframework.provider.jdbc.DDLFactory;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static store.jesframework.util.JdbcUtils.createConnection;
import static store.jesframework.util.PropsReader.getProperty;
import static java.sql.Connection.TRANSACTION_READ_COMMITTED;

@Slf4j
public class JdbcLock implements Lock {

    private final DataSource dataSource;

    public JdbcLock(@Nonnull DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "DataSource must not be null");

        try (Connection connection = createConnection(this.dataSource)) {
            createLocks(connection, DDLFactory.getLockDDL(connection));
        } catch (Exception e) {
            throw new BrokenStoreException(e);
        }
    }

    @SneakyThrows
    private void createLocks(@Nonnull Connection connection, @Nonnull String ddl) {
        try (PreparedStatement preparedStatement = connection.prepareStatement(ddl)) {
            final int code = preparedStatement.executeUpdate();
            if (code == 0) {
                log.info("Locks table successfully created");
            }
        }
    }

    @Override
    @SuppressWarnings("squid:S1141")
    public void doExclusively(@Nonnull String key, @Nonnull Runnable action) {
        Objects.requireNonNull(key, "Key must not be null");
        Objects.requireNonNull(action, "Action must not be null");

        try (Connection connection = createConnection(dataSource)) {
            try {
                connection.setAutoCommit(false);
                connection.setTransactionIsolation(TRANSACTION_READ_COMMITTED);
                final String lockQuery = getProperty("jes.jdbc.statement.insert-lock");
                final String unlockQuery = getProperty("jes.jdbc.statement.delete-lock");
                try (final PreparedStatement lockStatement = connection.prepareStatement(lockQuery);
                     final PreparedStatement unlockStatement = connection.prepareStatement(unlockQuery)) {

                    lockStatement.setString(1, key);
                    lockStatement.executeUpdate();

                    unlockStatement.setString(1, key);
                    unlockStatement.executeUpdate();
                }

                action.run();

                connection.commit();
            } catch (SQLException e) {
                connection.rollback();
                log.error("Lock was released with errors", e);
            }
        } catch (Exception e) {
            throw new BrokenStoreException(e);
        }
    }
}
