package org.fluentjdbc;

import static org.assertj.core.api.Java6Assertions.assertThat;
import static org.fluentjdbc.FluentJdbcAsserts.assertThat;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.Map;
import java.util.Random;

import org.fluentjdbc.h2.H2TestDatabase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class FluentJdbcDemonstrationTest extends AbstractDatabaseTest {

    private DatabaseTable table = new DatabaseTableWithTimestamps("demo_table");

    protected Connection connection;

    public FluentJdbcDemonstrationTest() throws SQLException {
        this(H2TestDatabase.createConnection(), H2TestDatabase.REPLACEMENTS);
    }

    protected FluentJdbcDemonstrationTest(Connection connection, Map<String, String> replacements) {
        super(replacements);
        this.connection = connection;
    }

    @Before
    public void createTables() throws SQLException {
        dropTableIfExists(connection, "demo_table");
        try(Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(preprocessCreateTable("create table demo_table (id ${INTEGER_PK}, code integer not null, name varchar(50) not null, updated_at ${DATETIME} not null, created_at ${DATETIME} not null)"));
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
                .execute(connection)
                .getId();

        String retrievedName = table.where("id", id).singleString(connection, "name");
        assertThat(retrievedName).isEqualTo(savedName);
    }

    @Test
    public void shouldUpdateRowWithExistingId() throws SQLException {
        String savedName = "demo row";
        Long id = table
                .newSaveBuilder("id", (Long)null)
                .uniqueKey("code", 123)
                .setField("name", savedName)
                .execute(connection)
                .getId();
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
    public void shouldInsertRowWithNonexistantKey() throws SQLException {
        String newRow = "Nonexistingent key";
        long pregeneratedId = 1000 + new Random().nextInt();
        Long id = table.newSaveBuilder("id", pregeneratedId)
                .uniqueKey("code", 235235)
                .setField("name", newRow)
                .execute(connection)
                .getId();

        assertThat(table.where("id", id).singleString(connection, "name"))
            .isEqualTo(newRow);
    }

    @Test
    public void shouldUpdateRowWithDuplicateUniqueKey() throws SQLException {
        String savedName = "old value";
        Long id = table.newSaveBuilder("id", null)
                .uniqueKey("code", 242112)
                .setField("name", savedName)
                .execute(connection)
                .getId();
        String updatedName = "updated name";
        table.newSaveBuilder("id", null)
                .uniqueKey("code", 242112)
                .setField("name", updatedName)
                .execute(connection);

        assertThat(table.where("id", id).singleString(connection, "name"))
            .isEqualTo(updatedName);
    }

    @Test
    public void shouldCreateTimestamps() throws InterruptedException, SQLException {
        Instant start = Instant.now();
        Thread.sleep(10);
        Long id = table
                .newSaveBuilder("id", (Long)null)
                .uniqueKey("code", 32352)
                .setField("name", "demo row")
                .execute(connection)
                .getId();
        Thread.sleep(10);

        assertThat(table.where("id", id).singleDateTime(connection, "created_at"))
            .isAfter(start).isBefore(Instant.now());
        assertThat(table.where("id", id).singleDateTime(connection, "updated_at"))
            .isAfter(start).isBefore(Instant.now());
    }

    @Test
    public void shouldUpdateTimestamp() throws InterruptedException, SQLException {
        Long id = table
                .newSaveBuilder("id", (Long)null)
                .uniqueKey("code", 32352)
                .setField("name", "demo row")
                .execute(connection)
                .getId();
        Instant createdTime = table.where("id", id).singleDateTime(connection, "updated_at");
        Instant updatedTime = table.where("id", id).singleDateTime(connection, "updated_at");
        Thread.sleep(10);

        table.newSaveBuilder("id", id).setField("name", "another value").execute(connection);
        assertThat(table.where("id", id).singleDateTime(connection, "updated_at"))
            .isAfter(updatedTime);
        assertThat(table.where("id", id).singleDateTime(connection, "created_at"))
            .isEqualTo(createdTime);
    }

    @Test
    public void shouldNotUpdateUnchangedRows() throws InterruptedException, SQLException {
        Long id = table
                .newSaveBuilder("id", (Long)null)
                .uniqueKey("code", 32352)
                .setField("name", "original value")
                .execute(connection)
                .getId();
        Instant updatedTime = table.where("id", id).singleDateTime(connection, "updated_at");
        Thread.sleep(10);

        table.newSaveBuilder("id", id).setField("name", "original value").execute(connection);
        assertThat(table.where("id", id).singleDateTime(connection, "updated_at"))
            .isEqualTo(updatedTime);
    }

}
