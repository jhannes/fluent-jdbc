package org.fluentjdbc.sqlite;

import org.fluentjdbc.AbstractDatabaseTest;
import org.fluentjdbc.DatabaseTable;
import org.fluentjdbc.DatabaseTableImpl;
import org.fluentjdbc.FluentJdbcAsserts;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

public class SqliteDatabaseTableTest extends AbstractDatabaseTest {

    public SqliteDatabaseTableTest() {
        super(SqliteTests.REPLACEMENTS);
    }

    private DatabaseTable table = new DatabaseTableImpl("demo_table");

    private Connection connection;

    @Test
    public void shouldInsertWithoutUsingPkGenerator() throws Exception {
        Object id = table.newSaveBuilderNoGeneratedKeys("id", null)
            .setField("code", 20001)
            .setField("name", "test insert without primary key generation").execute(connection).getId();
        assertThat(id).isNull();

        try (PreparedStatement statement = connection.prepareStatement("select last_insert_rowid()")) {
            try (ResultSet rs = statement.executeQuery()) {
                rs.next();
                id = rs.getInt(1);
            }
        }

        assertThat(table.where("id", id).singleString(connection, "name")).get()
            .isEqualTo("test insert without primary key generation");
    }

    @Test
    public void shouldInsertWithAssignedId() throws Exception {
        Long id = table.newSaveBuilderNoGeneratedKeys("id", 21912L)
            .setField("code", 20002)
            .setField("name", "test insert with original id")
            .execute(connection)
            .getId();
        assertThat(id).isEqualTo(21912L);

        assertThat(table.where("id", 21912L).singleString(connection, "name")).get()
            .isEqualTo("test insert with original id");
    }

    @Test
    public void shouldUpdateWithAssignedId() throws Exception {
        table.newSaveBuilderNoGeneratedKeys("id", null)
            .setField("code", 20003)
            .setField("name", "test before update").execute(connection);

        long id;
        try (PreparedStatement statement = connection.prepareStatement("select last_insert_rowid()")) {
            try (ResultSet rs = statement.executeQuery()) {
                rs.next();
                id = rs.getInt(1);
            }
        }

        table.newSaveBuilder("id", id)
            .setField("code", 20003)
            .setField("name", "test after update")
            .execute(connection);

        assertThat(table.where("id", id).singleString(connection, "name")).get()
            .isEqualTo("test after update");
    }

    @Before
    public void openConnection() throws SQLException {
        connection = SqliteTests.getConnection();

        dropTableIfExists(connection, "demo_table");
        try(Statement stmt = connection.createStatement()) {
            stmt.executeUpdate("create table demo_table (id integer primary key autoincrement, code integer not null, name varchar not null)");
        }
    }

}
