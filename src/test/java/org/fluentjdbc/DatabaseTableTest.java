package org.fluentjdbc;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class DatabaseTableTest {

    private DatabaseTable table = new DatabaseTableImpl("demo_table");

    private Connection connection;

    private DatabaseTable missingTable = new DatabaseTableImpl("non_existing");

    @Before
    public void openConnection() throws SQLException {
        String jdbcUrl = System.getProperty("test.db.jdbc_url", "jdbc:h2:mem:" + getClass().getName());
        connection = DriverManager.getConnection(jdbcUrl);

        try(Statement stmt = connection.createStatement()) {
            stmt.executeUpdate("drop table if exists demo_table");
            stmt.executeUpdate(preprocessCreateTable(connection,
                    "create table demo_table (id integer primary key auto_increment, code integer not null, name varchar not null)"));
        }
    }

    private static String preprocessCreateTable(Connection connection, String createTableStatement) throws SQLException {
        String productName = connection.getMetaData().getDatabaseProductName();
        if (productName.equals("SQLite")) {
            return createTableStatement.replaceAll("auto_increment", "autoincrement");
        } else {
            return createTableStatement;
        }
    }


    @Test
    public void shouldThrowOnMissingColumn() throws Exception {
        long id = table.insert().setField("code", 1234).setField("name", "testing").generateKeyAndInsert(connection);

        assertThatThrownBy(() -> table.where("id", id).singleString(connection, "non_existing"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Column {non_existing} is not present");
    }

    @Test
    public void shouldThrowOnMissingTable() {
        assertThatThrownBy(() -> missingTable.where("id", 12).singleLong(connection, "id"))
            .isInstanceOf(SQLException.class);
        assertThatThrownBy(() -> missingTable.where("id", 12).list(connection, row -> row.getLong("id")))
            .isInstanceOf(SQLException.class);
        assertThatThrownBy(() -> missingTable.insert().setField("id", 12).execute(connection))
            .isInstanceOf(SQLException.class);
        assertThatThrownBy(() -> missingTable.insert().setField("name", "abc").generateKeyAndInsert(connection))
            .isInstanceOf(SQLException.class);
    }

    @Test
    public void shouldThrowIfSingleQueryReturnsMultipleRows() {
        table.insert().setField("code", 123).setField("name", "the same name").execute(connection);
        table.insert().setField("code", 456).setField("name", "the same name").execute(connection);

        assertThatThrownBy(() -> table.where("name", "the same name").singleLong(connection, "code"))
            .isInstanceOf(IllegalStateException.class);

    }

}
