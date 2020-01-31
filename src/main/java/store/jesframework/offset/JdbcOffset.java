package store.jesframework.offset;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.sql.DataSource;

import store.jesframework.ex.BrokenStoreException;
import store.jesframework.provider.jdbc.DDLFactory;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static store.jesframework.util.JdbcUtils.createConnection;
import static store.jesframework.util.PropsReader.getProperty;

@Slf4j
public class JdbcOffset implements Offset {

    private final DataSource dataSource;

    public JdbcOffset(@Nonnull DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "Datasource must not be null");

        try (final Connection connection = createConnection(dataSource)) {
            createOffsetTable(connection, DDLFactory.getOffsetsDDL(connection));
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
        final String query = getProperty("jes.jdbc.statement.select-offset");
        try (final Connection connection = createConnection(dataSource);
             final PreparedStatement statement = connection.prepareStatement(query)) {
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
    public void add(@Nonnull String key, long value) {
        Objects.requireNonNull(key);
        if (!addToOffsetByKey(key, value)) {
            log.trace("Offset [" + key + "] not found. Creating new one...");
            synchronized (this) {
                if (!addToOffsetByKey(key, value)) {
                    createOffset(key);
                }
            }
            if (!addToOffsetByKey(key, value)) {
                throw new BrokenStoreException("Can't add value to offset by key " + key);
            }
        }
    }

    @Override
    public void increment(@Nonnull String key) {
        Objects.requireNonNull(key);
        if (!incrementOffsetByKey(key)) {
            log.trace("Offset [" + key + "] not found. Creating new one...");
            synchronized (this) {
                if (!incrementOffsetByKey(key)) {
                    createOffset(key);
                }
            }
            if (!incrementOffsetByKey(key)) {
                throw new BrokenStoreException("Can't increment offset by key " + key);
            }
        }
    }

    @Override
    public void reset(@Nonnull String key) {
        Objects.requireNonNull(key);
        final String query = getProperty("jes.jdbc.statement.update-offset-and-reset");
        try (final Connection connection = createConnection(dataSource);
             final PreparedStatement statement = connection.prepareStatement(query)) {

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
        final String query = getProperty("jes.jdbc.statement.update-offset-and-increment");
        try (final Connection connection = createConnection(dataSource);
             final PreparedStatement statement = connection.prepareStatement(query)) {

            statement.setString(1, key);
            return statement.executeUpdate() == 1;
        } catch (Exception e) {
            throw new BrokenStoreException(e);
        }
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    private boolean addToOffsetByKey(@Nonnull String key, long value) {
        final String query = getProperty("jes.jdbc.statement.update-offset-and-add");
        try (final Connection connection = createConnection(dataSource);
             final PreparedStatement statement = connection.prepareStatement(query)) {

            statement.setString(2, key);
            statement.setLong(1, value);
            return statement.executeUpdate() == 1;
        } catch (Exception e) {
            throw new BrokenStoreException(e);
        }
    }

    private void createOffset(@Nonnull String key) {
        final String query = getProperty("jes.jdbc.statement.insert-offset");
        try (final Connection connection = createConnection(dataSource);
             final PreparedStatement statement = connection.prepareStatement(query)) {

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
