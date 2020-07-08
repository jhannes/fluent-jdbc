package org.fluentjdbc;

import org.junit.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class DbContextConnectionTest {

    private Connection connectionMock = Mockito.mock(Connection.class);

    @Test
    public void shouldThrowWhenConnectionFails() {
        SQLException exception = new SQLException("Connection failed!");
        DbContext.TopLevelDbContextConnection connection = new DbContext.TopLevelDbContextConnection(() -> { throw exception; }, new DbContext());
        assertThatThrownBy(connection::getConnection).isEqualTo(exception);
    }

    @Test
    public void shouldThrowWhenCloseFails() throws SQLException {
        DbContext.TopLevelDbContextConnection connection = new DbContext.TopLevelDbContextConnection(() -> connectionMock, new DbContext());
        connection.getConnection();

        Mockito.doThrow(new SQLException("Failed to close")).when(connectionMock).close();
        assertThatThrownBy(connection::close)
                .isInstanceOf(SQLException.class);
    }
}
