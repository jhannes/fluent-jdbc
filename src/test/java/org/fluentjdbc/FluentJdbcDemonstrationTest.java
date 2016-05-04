package org.fluentjdbc;

import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

import static org.fluentjdbc.FluentJdbcAsserts.assertThat;

import org.fluentjdbc.h2.H2TestDatabase;

public class FluentJdbcDemonstrationTest {

    private DatabaseTable table = new DatabaseTableWithTimestamps("demo_table");

    private Connection connection;

    public FluentJdbcDemonstrationTest() throws SQLException {
        this(H2TestDatabase.createConnection());
    }

    protected FluentJdbcDemonstrationTest(Connection connection) {
        this.connection = connection;
    }

    @Before
    public void createTables() throws SQLException {
        try(Statement stmt = connection.createStatement()) {
            stmt.executeUpdate("drop table if exists demo_table");
            stmt.executeUpdate(preprocessCreateTable(connection,
                    "create table demo_table (id integer primary key auto_increment, code integer not null, name varchar not null, updated_at datetime not null, created_at datetime not null)"));
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

    @After
    public void closeConnection() throws SQLException {
        connection.close();
    }

    @Test
    public void shouldGenerateIdForNewRow() throws Exception {
        String savedName = "demo row";
        Long id = table
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
        Long id = table
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
        Long id = table.newSaveBuilder("id", pregeneratedId)
                .uniqueKey("code", 235235)
                .setField("name", newRow)
                .execute(connection);

        assertThat(table.where("id", id).singleString(connection, "name"))
            .isEqualTo(newRow);
    }

    @Test
    public void shouldUpdateRowWithDuplicateUniqueKey() {
        String savedName = "old value";
        Long id = table.newSaveBuilder("id", null)
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

    @Test
    public void shouldCreateTimestamps() throws InterruptedException {
        DateTime start = DateTime.now();
        Thread.sleep(10);
        Long id = table
                .newSaveBuilder("id", (Long)null)
                .uniqueKey("code", 32352)
                .setField("name", "demo row")
                .execute(connection);
        Thread.sleep(10);

        assertThat(table.where("id", id).singleDateTime(connection, "created_at"))
            .isAfter(start).isBefore(DateTime.now());
        assertThat(table.where("id", id).singleDateTime(connection, "updated_at"))
            .isAfter(start).isBefore(DateTime.now());
    }

    @Test
    public void shouldUpdateTimestamp() throws InterruptedException {
        Long id = table
                .newSaveBuilder("id", (Long)null)
                .uniqueKey("code", 32352)
                .setField("name", "demo row")
                .execute(connection);
        DateTime createdTime = table.where("id", id).singleDateTime(connection, "updated_at");
        DateTime updatedTime = table.where("id", id).singleDateTime(connection, "updated_at");
        Thread.sleep(10);

        table.newSaveBuilder("id", id).setField("name", "another value").execute(connection);
        assertThat(table.where("id", id).singleDateTime(connection, "updated_at"))
            .isAfter(updatedTime);
        assertThat(table.where("id", id).singleDateTime(connection, "created_at"))
            .isEqualTo(createdTime);
    }

    @Test
    public void shouldNotUpdateUnchangedRows() throws InterruptedException {
        Long id = table
                .newSaveBuilder("id", (Long)null)
                .uniqueKey("code", 32352)
                .setField("name", "original value")
                .execute(connection);
        DateTime updatedTime = table.where("id", id).singleDateTime(connection, "updated_at");
        Thread.sleep(10);

        table.newSaveBuilder("id", id).setField("name", "original value").execute(connection);
        assertThat(table.where("id", id).singleDateTime(connection, "updated_at"))
            .isEqualTo(updatedTime);
    }

}
