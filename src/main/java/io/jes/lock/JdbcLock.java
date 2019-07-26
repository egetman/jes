package io.jes.lock;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.sql.DataSource;

import io.jes.ex.BrokenStoreException;
import io.jes.provider.jdbc.DDLFactory;
import io.jes.provider.jdbc.LockDDLProducer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static java.sql.Connection.*;

@Slf4j
public class JdbcLock implements Lock {

    private final DataSource dataSource;
    private final LockDDLProducer ddlProducer;

    @SuppressWarnings("WeakerAccess")
    public JdbcLock(@Nonnull DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "DataSouce must not be null");
        try (Connection connection = dataSource.getConnection()) {
            this.ddlProducer = DDLFactory.newLockDDLProducer(connection);
            createLocks(connection, ddlProducer.createLockTable());
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
    public void doProtectedWrite(@Nonnull String key, @Nonnull Runnable action) {
        Objects.requireNonNull(key, "Key must not be null");
        Objects.requireNonNull(action, "Action must not be null");

        try (Connection connection = dataSource.getConnection()) {
            try {
                connection.setAutoCommit(false);
                connection.setTransactionIsolation(TRANSACTION_READ_COMMITTED);
                try (final PreparedStatement lockStatement = connection.prepareStatement(ddlProducer.lock());
                     final PreparedStatement unlockStatement = connection.prepareStatement(ddlProducer.unlock())) {

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
