package org.fluentjdbc;

import static org.assertj.core.api.Java6Assertions.assertThat;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

public class DatabaseSaveBuilderTest {

    private DatabaseTable table = new DatabaseTableWithTimestamps("uuid_table");

    private Connection connection;

    public DatabaseSaveBuilderTest() throws SQLException {
        this(createConnection());
    }

    protected DatabaseSaveBuilderTest(Connection connection) {
        this.connection = connection;
    }

    private static Connection createConnection() throws SQLException {
        String jdbcUrl = System.getProperty("test.db.jdbc_url", "jdbc:h2:mem:DatabaseSaveBuilderTest");
        return DriverManager.getConnection(jdbcUrl);
    }

    @Before
    public void openConnection() throws SQLException {
        try(Statement stmt = connection.createStatement()) {
            stmt.executeUpdate("drop table if exists uuid_table");
            stmt.executeUpdate(
                    "create table uuid_table (id uuid primary key, code integer not null, name varchar not null, updated_at datetime not null, created_at datetime not null)");
        }
    }

    @After
    public void closeConnection() throws SQLException {
        connection.close();
    }


    @Test
    public void shouldGenerateIdForNewRow() throws Exception {
        String savedName = "demo row";
        UUID id = table
                .newSaveBuilderWithUUID("id", null)
                .uniqueKey("code", 123)
                .setField("name", savedName)
                .execute(connection);

        String retrievedName = table.where("id", id).singleString(connection, "name");
        assertThat(retrievedName).isEqualTo(savedName);
    }


    @Test
    public void shouldUpdateRow() throws Exception {
        String savedName = "original row";
        UUID id = table
                .newSaveBuilderWithUUID("id", null)
                .uniqueKey("code", 123)
                .setField("name", savedName)
                .execute(connection);

        table.newSaveBuilderWithUUID("id", id)
            .uniqueKey("code", 543)
            .setField("name", "updated value")
            .execute(connection);

        String retrievedName = table.where("id", id).singleString(connection, "name");
        assertThat(retrievedName).isEqualTo("updated value");
    }

    @Test
    public void shouldUpdateRowOnKey() {
        UUID idOnInsert = table.newSaveBuilderWithUUID("id", null)
            .uniqueKey("code", 10001)
            .setField("name", "old name")
            .execute(connection);

        UUID idOnUpdate = table.newSaveBuilderWithUUID("id", null)
            .uniqueKey("code", 10001)
            .setField("name", "new name")
            .execute(connection);

        assertThat(idOnInsert).isEqualTo(idOnUpdate);
        assertThat(table.where("id", idOnInsert).singleString(connection, "name"))
            .isEqualTo("new name");
    }

    @Test
    public void shouldGenerateUsePregeneratedIdForNewRow() throws Exception {
        String savedName = "demo row";
        UUID id = UUID.randomUUID();
        UUID generatedKey = table
                .newSaveBuilderWithUUID("id", id)
                .uniqueKey("code", 123)
                .setField("name", savedName)
                .execute(connection);
        assertThat(id).isEqualTo(generatedKey);

        String retrievedName = table.where("id", id).singleString(connection, "name");
        assertThat(retrievedName).isEqualTo(savedName);
    }

}
