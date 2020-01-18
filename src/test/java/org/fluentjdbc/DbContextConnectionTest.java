package org.fluentjdbc;

import org.junit.Test;

import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class DbContextConnectionTest {

    @Test
    public void shouldThrowWhenConnectionFails() {
        SQLException exception = new SQLException("Connection failed!");
        DbContextConnection connection = new DbContextConnection(() -> { throw exception; }, new DbContext());
        assertThatThrownBy(connection::getConnection).isEqualTo(exception);
    }
}
