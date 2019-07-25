package io.jes.offset;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.sql.DataSource;

import io.jes.ex.BrokenStoreException;
import io.jes.provider.jdbc.DDLFactory;
import io.jes.provider.jdbc.OffsetDDLProducer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcOffset implements Offset {

    private final DataSource dataSource;
    private final OffsetDDLProducer ddlProducer;

    public JdbcOffset(@Nonnull DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "Datasource must not be null");

        try (final Connection connection = dataSource.getConnection()) {
            this.ddlProducer = DDLFactory.newOffsetDDLProducer(connection);
            createOffsetTable(connection, ddlProducer.createOffsetTable());
        } catch (Exception e) {
            throw new BrokenStoreException("Failed to create " + getClass().getSimpleName(), e);
        }
    }

    @SneakyThrows
    private void createOffsetTable(@Nonnull Connection connection, @Nonnull String ddl) {
        try (PreparedStatement preparedStatement = connection.prepareStatement(ddl)) {
            final int code = preparedStatement.executeUpdate();
            if (code == 0) {
                log.info("Offset table successfully created");
            }
        }
    }

    @Override
    public long value(@Nonnull String key) {
        Objects.requireNonNull(key);
        try (final Connection connection = dataSource.getConnection();
             final PreparedStatement statement = connection.prepareStatement(ddlProducer.value())) {
            statement.setString(1, key);
            try (final ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    // there is no offset by given key, so it's ok to return initial value
                    return 0;
                }
                return resultSet.getLong(1);
            }
        } catch (Exception e) {
            throw new BrokenStoreException(e);
        }
    }

    @Override
    public void increment(@Nonnull String key) {
        Objects.requireNonNull(key);
        if (!incrementOffsetByKey(key)) {
            log.warn("Offset [" + key + "] not found. Creating new one...");
            createOffset(key);
            if (!incrementOffsetByKey(key)) {
                throw new BrokenStoreException("Can't increment offset by key " + key);
            }
        }
    }

    @Override
    public void reset(@Nonnull String key) {
        Objects.requireNonNull(key);
        try (final Connection connection = dataSource.getConnection();
             final PreparedStatement statement = connection.prepareStatement(ddlProducer.reset())) {

            statement.setString(1, key);
            final int code = statement.executeUpdate();
            if (code == 1) {
                log.debug("Offset by key [" + key + "] successfully set to 0");
            }
        } catch (Exception e) {
            throw new BrokenStoreException(e);
        }
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    private boolean incrementOffsetByKey(@Nonnull String key) {
        try (final Connection connection = dataSource.getConnection();
             final PreparedStatement statement = connection.prepareStatement(ddlProducer.increment())) {

            statement.setString(1, key);
            return statement.executeUpdate() == 1;
        } catch (Exception e) {
            throw new BrokenStoreException(e);
        }
    }

    private void createOffset(@Nonnull String key) {
        try (final Connection connection = dataSource.getConnection();
             final PreparedStatement statement = connection.prepareStatement(ddlProducer.createOffset())) {

            statement.setString(1, key);
            final int count = statement.executeUpdate();
            if (count == 1) {
                log.debug("Offset by key [" + key + "] successfully created");
            }
        } catch (Exception e) {
            throw new BrokenStoreException(e);
        }
    }

}
