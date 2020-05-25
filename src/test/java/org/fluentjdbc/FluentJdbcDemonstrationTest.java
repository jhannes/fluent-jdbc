package org.fluentjdbc;

import org.fluentjdbc.h2.H2TestDatabase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.Map;
import java.util.Random;

import static org.fluentjdbc.FluentJdbcAsserts.assertThat;
import static org.fluentjdbc.FluentJdbcAsserts.assertThatOptional;

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
                .newSaveBuilder("id", null)
                .uniqueKey("code", 123)
                .setField("name", savedName)
                .execute(connection)
                .getId();

        assertThat(table.where("id", id).singleString(connection, "name")).get().isEqualTo(savedName);
    }

    @Test
    public void shouldUpdateRowWithExistingId() throws SQLException {
        String savedName = "demo row";
        Long id = table
                .newSaveBuilder("id", null)
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

        assertThat(table.where("id", id).singleString(connection, "name")).get().isEqualTo(updatedName);
        assertThat(table.where("id", id).singleLong(connection, "code")).get().isEqualTo(543L);
    }

    @Test
    public void shouldInsertRowWithNonexistentKey() throws SQLException {
        String newRow = "Nonexistent key";
        long pregeneratedId = 1000 + new Random().nextInt();
        Long id = table.newSaveBuilder("id", pregeneratedId)
                .uniqueKey("code", 235235)
                .setField("name", newRow)
                .execute(connection)
                .getId();

        assertThat(table.where("id", id).singleString(connection, "name")).get()
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

        assertThat(table.where("id", id).singleString(connection, "name")).get()
            .isEqualTo(updatedName);
    }

    @Test
    public void shouldCreateTimestamps() throws InterruptedException, SQLException {
        Instant start = Instant.now();
        Thread.sleep(10);
        Long id = table
                .newSaveBuilder("id", null)
                .uniqueKey("code", 32352)
                .setField("name", "demo row")
                .execute(connection)
                .getId();
        Thread.sleep(10);

        assertThatOptional(table.where("id", id).singleInstant(connection, "created_at"))
            .isAfter(start).isBefore(Instant.now());
        assertThatOptional(table.where("id", id).singleInstant(connection, "updated_at"))
            .isAfter(start).isBefore(Instant.now());
    }

    @Test
    public void shouldUpdateTimestamp() throws InterruptedException, SQLException {
        Long id = table
                .newSaveBuilder("id", null)
                .uniqueKey("code", 32352)
                .setField("name", "demo row")
                .execute(connection)
                .getId();
        Instant createdTime = table.where("id", id).singleInstant(connection, "updated_at")
                .orElseThrow(IllegalArgumentException::new);
        Instant updatedTime = table.where("id", id).singleInstant(connection, "updated_at")
                .orElseThrow(IllegalArgumentException::new);
        Thread.sleep(10);

        table.newSaveBuilder("id", id).setField("name", "another value").execute(connection);
        assertThatOptional(table.where("id", id).singleInstant(connection, "updated_at"))
            .isAfter(updatedTime);
        assertThatOptional(table.where("id", id).singleInstant(connection, "created_at"))
            .isEqualTo(createdTime);
    }

    @Test
    public void shouldNotUpdateUnchangedRows() throws InterruptedException, SQLException {
        Long id = table
                .newSaveBuilder("id", null)
                .uniqueKey("code", 32352)
                .setField("name", "original value")
                .execute(connection)
                .getId();
        Instant updatedTime = table.where("id", id).singleInstant(connection, "updated_at")
                .orElseThrow(IllegalArgumentException::new);
        Thread.sleep(10);

        table.newSaveBuilder("id", id).setField("name", "original value").execute(connection);
        assertThat(table.where("id", id).singleInstant(connection, "updated_at")).get()
            .isEqualTo(updatedTime);
    }
}
