package org.fluentjdbc;

import static org.assertj.core.api.Assertions.assertThat;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

public class FluentJdbcDemonstrationTest {


    private DatabaseTable table = new DatabaseTableWithTimestamps("demo_table");

    private Connection connection;

    @Before
    public void openConnection() throws SQLException {
        String jdbcUrl = System.getProperty("test.db.jdbc_url", "jdbc:h2:mem:" + getClass().getName());

        if (jdbcUrl.startsWith("jdbc:h2:")) {
            JdbcDataSource dataSource = new JdbcDataSource();
            dataSource.setUrl(jdbcUrl);

            connection = dataSource.getConnection();
            try(Statement stmt = connection.createStatement()) {
                stmt.executeUpdate("drop table if exists demo_table");
                stmt.executeUpdate("create table demo_table (id integer primary key auto_increment, code integer not null, name varchar not null)");
            }
        } else {
            connection = DriverManager.getConnection(jdbcUrl);
            try(Statement stmt = connection.createStatement()) {
                stmt.executeUpdate("drop table if exists demo_table");
                stmt.executeUpdate("create table demo_table (id integer primary key autoincrement, code integer not null, name varchar not null)");
            }
        }
    }

    @After
    public void closeConnection() throws SQLException {
        connection.close();
    }

    @Test
    public void shouldGenerateIdForNewRow() throws Exception {
        String savedName = "demo row";
        long id = table
                .newSaveBuilder("id", (Long)null)
                .uniqueKey("code", 123)
                .setField("name", savedName)
                .execute(connection);

        String retrievedName = table.where("id", id).singleString(connection, "name");
        assertThat(retrievedName).isEqualTo(savedName);
    }

    @Test
    public void shouldUpdateRowWithExistingId() {
        String savedName = "demo row";
        long id = table
                .newSaveBuilder("id", (Long)null)
                .uniqueKey("code", 123)
                .setField("name", savedName)
                .execute(connection);
        String updatedName = "updated name";
        table
                .newSaveBuilder("id", id)
                .uniqueKey("code", 543)
                .setField("name", updatedName)
                .execute(connection);

        String retrievedName = table.where("id", id).singleString(connection, "name");
        assertThat(retrievedName).isEqualTo(updatedName);
        assertThat(table.where("id", id).singleLong(connection, "code")).isEqualTo(543L);
    }

    @Test
    public void shouldInsertRowWithNonexistantKey() {
        String newRow = "Nonexistingent key";
        long pregeneratedId = 1000 + new Random().nextInt();
        long id = table.newSaveBuilder("id", pregeneratedId)
                .uniqueKey("code", 235235)
                .setField("name", newRow)
                .execute(connection);

        assertThat(table.where("id", id).singleString(connection, "name"))
            .isEqualTo(newRow);
    }

    @Test
    public void shouldUpdateRowWithDuplicateUniqueKey() {
        String savedName = "old value";
        long id = table.newSaveBuilder("id", null)
                .uniqueKey("code", 242112)
                .setField("name", savedName)
                .execute(connection);
        String updatedName = "updated name";
        table.newSaveBuilder("id", null)
                .uniqueKey("code", 242112)
                .setField("name", updatedName)
                .execute(connection);

        assertThat(table.where("id", id).singleString(connection, "name"))
            .isEqualTo(updatedName);
    }


}
