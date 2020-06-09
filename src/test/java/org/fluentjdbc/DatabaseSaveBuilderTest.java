package org.fluentjdbc;

import org.fluentjdbc.h2.H2TestDatabase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.fluentjdbc.DatabaseSaveResult.SaveStatus.INSERTED;
import static org.fluentjdbc.DatabaseSaveResult.SaveStatus.UPDATED;

public class DatabaseSaveBuilderTest extends AbstractDatabaseTest {

    private DatabaseTable table = new DatabaseTableWithTimestamps("uuid_table");

    private Connection connection;

    public DatabaseSaveBuilderTest() throws SQLException {
        this(createConnection(), H2TestDatabase.REPLACEMENTS);
    }

    protected DatabaseSaveBuilderTest(Connection connection, Map<String, String> replacements) {
        super(replacements);
        this.connection = connection;
    }

    private static Connection createConnection() throws SQLException {
        String jdbcUrl = System.getProperty("test.db.jdbc_url", "jdbc:h2:mem:DatabaseSaveBuilderTest");
        return DriverManager.getConnection(jdbcUrl);
    }

    @Before
    public void openConnection() throws SQLException {
        dropTableIfExists(connection, "uuid_table");
        try(Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(
                    preprocessCreateTable("create table uuid_table (id ${UUID} primary key, code integer not null, name varchar(50) not null, expired_at ${DATETIME}, updated_at ${DATETIME} not null, created_at ${DATETIME} not null)"));
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
                .execute(connection)
                .getId();

        assertThat(table.where("id", id).singleString(connection, "name")).get().isEqualTo(savedName);
    }

    @Test
    public void shouldUpdateRow() throws Exception {
        String savedName = "original row";
        UUID id = table
                .newSaveBuilderWithUUID("id", null)
                .uniqueKey("code", 123)
                .setField("name", savedName)
                .execute(connection)
                .getId();

        DatabaseSaveResult<UUID> result = table.newSaveBuilderWithUUID("id", id)
                .uniqueKey("code", 543)
                .setField("name", "updated value")
                .execute(connection);
        assertThat(result.getSaveStatus()).isEqualTo(UPDATED);

        assertThat(table.where("id", id).singleString(connection, "name")).get().isEqualTo("updated value");

        assertThat(table.where("id", id)
                .orderBy("name")
                .list(connection, row -> row.getUUID("id")))
            .containsOnly(id);
    }

    @Test
    public void shouldNotUpdateUnchangedRow() throws SQLException {
        Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        String savedName = "original row";
        UUID firstId = table
                .newSaveBuilderWithUUID("id", null)
                .uniqueKey("code", 123)
                .setField("name", savedName)
                .setField("expired_at", now)
                .execute(connection)
                .getId();

        DatabaseSaveResult<UUID> result = table
                .newSaveBuilderWithUUID("id", null)
                .uniqueKey("code", 123)
                .setField("name", savedName)
                .setField("expired_at", now)
                .execute(connection);
        assertThat(result).isEqualTo(DatabaseSaveResult.unchanged(firstId));
    }

    @Test
    public void shouldUpdateRowOnKey() throws SQLException {
        UUID idOnInsert = table.newSaveBuilderWithUUID("id", null)
            .uniqueKey("code", 10001)
            .setField("name", "old name")
            .execute(connection)
            .getId();

        DatabaseSaveResult<UUID> result = table.newSaveBuilderWithUUID("id", null)
                .uniqueKey("code", 10001)
                .setField("name", "new name")
                .execute(connection);
        assertThat(result).isEqualTo(DatabaseSaveResult.updated(idOnInsert, Collections.singletonList("name")));
        assertThat(result.getUpdatedFields()).isEqualTo(Collections.singletonList("name"));

        assertThat(table.where("id", idOnInsert).singleString(connection, "name")).get()
            .isEqualTo("new name");
    }

    @Test
    public void shouldGenerateUsePregeneratedIdForNewRow() throws Exception {
        String savedName = "demo row";
        UUID id = UUID.randomUUID();
        DatabaseSaveResult<UUID> result = table
                .newSaveBuilderWithUUID("id", id)
                .uniqueKey("code", 123)
                .setField("name", savedName)
                .execute(connection);
        assertThat(result.getSaveStatus()).isEqualTo(INSERTED);
        UUID generatedKey = result.getId();
        assertThat(id).isEqualTo(generatedKey);

        assertThat(table.where("id", id).singleString(connection, "name"))
                .get().isEqualTo(savedName);
    }

}
