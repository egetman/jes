package store.jesframework.offset;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import javax.annotation.Nonnull;
import javax.sql.DataSource;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import lombok.SneakyThrows;
import store.jesframework.ex.BrokenStoreException;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static store.jesframework.internal.FancyStuff.newMySqlDataSource;
import static store.jesframework.internal.FancyStuff.newPostgresDataSource;
import static store.jesframework.internal.FancyStuff.newRedissonClient;

class OffsetTest {

    private static final List<Offset> OFFSETS = asList(
            new InMemoryOffset(),
            new RedisOffset(newRedissonClient()),
            new JdbcOffset(newPostgresDataSource()),
            new JdbcOffset(newMySqlDataSource("es"))
    );

    private static Collection<Offset> offsets() {
        return OFFSETS;
    }

    @ParameterizedTest
    @MethodSource("offsets")
    @SuppressWarnings("ConstantConditions")
    void shouldThrowNpeOnNullArguments(@Nonnull Offset offset) {
        assertThrows(NullPointerException.class, () -> offset.value(null));
        assertThrows(NullPointerException.class, () -> offset.increment(null));
        assertThrows(NullPointerException.class, () -> offset.reset(null));
    }

    @ParameterizedTest
    @MethodSource("offsets")
    void shouldIncrementOffsetValueByKey(@Nonnull Offset offset) {
        final String key = getClass().getName();

        assertEquals(0, offset.value(key));
        offset.increment(key);
        assertEquals(1, offset.value(key));
        offset.increment(key);
        assertEquals(2, offset.value(key));
        assertEquals(0, offset.value(UUID.randomUUID().toString()));
    }

    @ParameterizedTest
    @MethodSource("offsets")
    void shouldResetOffsetByKey(@Nonnull Offset offset) {
        final String first = UUID.randomUUID().toString();
        final String second = UUID.randomUUID().toString();

        offset.increment(first);
        offset.increment(first);
        offset.increment(second);

        assertEquals(2, offset.value(first));
        assertEquals(1, offset.value(second));

        offset.reset(first);

        assertEquals(0, offset.value(first));
        assertEquals(1, offset.value(second));
    }

    @Test
    @SneakyThrows
    void sqlExceptionsInJdbcOffsetShouldBeWrappedInBrokenStoreException() {
        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        when(dataSource.getConnection()).thenReturn(connection);

        // first verify creation
        assertThrows(BrokenStoreException.class, () -> new JdbcOffset(dataSource));

        final DatabaseMetaData metadata = mock(DatabaseMetaData.class);
        when(connection.getMetaData()).thenReturn(metadata);
        when(metadata.getDatabaseProductName()).thenReturn("PostgreSQL");
        final PreparedStatement statement = mock(PreparedStatement.class);
        when(connection.prepareStatement(anyString())).thenReturn(statement);

        // ok, create new one, try all other methods
        final JdbcOffset offset = new JdbcOffset(dataSource);
        when(statement.executeUpdate()).thenThrow(new SQLException("Test exception"));

        assertThrows(BrokenStoreException.class, () -> offset.add("", 1));
        assertThrows(BrokenStoreException.class, () -> offset.increment(""));
        assertThrows(BrokenStoreException.class, () -> offset.reset(""));
        assertThrows(BrokenStoreException.class, () -> offset.value(""));
    }

    @AfterAll
    @SneakyThrows
    static void closeResources() {
        for (Offset offset : OFFSETS) {
            if (offset instanceof AutoCloseable) {
                try {
                    ((AutoCloseable) offset).close();
                } catch (Exception ignored) {}
            }
        }
    }

}