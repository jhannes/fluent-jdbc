package org.fluentjdbc;

import org.fluentjdbc.h2.H2TestDatabase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.fluentjdbc.DatabaseSaveResult.SaveStatus.INSERTED;
import static org.fluentjdbc.DatabaseSaveResult.SaveStatus.UPDATED;

public class DatabaseSaveBuilderTest extends AbstractDatabaseTest {

    private final DatabaseTable table = new DatabaseTableWithTimestamps("uuid_table");
    private final DatabaseTable multikeyTable = new DatabaseTableImpl("multikey_table");

    private final Connection connection;

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
    public void openConnection() {
        dropTableIfExists(connection, "uuid_table");
        createTable(connection, "create table uuid_table (id ${UUID} primary key, code integer not null, name varchar(50) not null, expired_at ${DATETIME}, updated_at ${DATETIME} not null, created_at ${DATETIME} not null)");
        dropTableIfExists(connection, "multikey_table");
        createTable(connection, "create table multikey_table (id ${UUID} primary key, first_name varchar(50) not null, last_name varchar(50) not null, address varchar(1000), unique (first_name, last_name))");
    }

    @After
    public void closeConnection() throws SQLException {
        connection.close();
    }


    @Test
    public void shouldGenerateIdForNewRow() {
        String savedName = "demo row";
        UUID id = table
                .newSaveBuilderWithUUID("id", null)
                .uniqueKey("code", 123)
                .setField("name", savedName)
                .execute(connection)
                .getId();

        assertThat(table.where("id", id).singleString(connection, "name").get()).isEqualTo(savedName);
    }

    @Test
    public void shouldUpdateRow() {
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

        assertThat(table.where("id", id).singleString(connection, "name").get()).isEqualTo("updated value");

        assertThat(table.where("id", id)
                .orderBy("name")
                .list(connection, row -> row.getUUID("id")))
            .containsOnly(id);
    }

    @Test
    public void shouldNotUpdateUnchangedRow() {
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
        assertThat(result.isChanged()).isFalse();
    }

    @Test
    public void shouldUpdateRowOnKey() {
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
        assertThat(result.toString()).contains(result.getId().toString());
        assertThat(result.hashCode()).isEqualTo(DatabaseSaveResult.updated(result.getId(), null).hashCode());

        assertThat(table.where("id", idOnInsert).singleString(connection, "name").get())
            .isEqualTo("new name");
    }

    @Test
    public void shouldGenerateUsePreGeneratedIdForNewRow() {
        String savedName = "demo row";
        UUID id = UUID.randomUUID();
        DatabaseSaveResult<UUID> result = table
                .newSaveBuilderWithUUID("id", id)
                .uniqueKey("code", 123)
                .setField("name", savedName)
                .execute(connection);
        assertThat(result.getSaveStatus()).isEqualTo(INSERTED);
        assertThat(result.isChanged()).isTrue();
        UUID generatedKey = result.getId();
        assertThat(id).isEqualTo(generatedKey);

        assertThat(table.where("id", id).singleString(connection, "name").get())
                .isEqualTo(savedName);
    }
    
    @Test
    public void shouldInsertMultipleRowsOnPartialKeyMatch() {
        DatabaseSaveResult<UUID> first = multikeyTable.newSaveBuilderWithUUID("id", null)
                .uniqueKey("first_name", "John")
                .uniqueKey("last_name", "Doe")
                .setField("address", "Database St 1")
                .execute(connection);
        assertThat(first.getSaveStatus()).isEqualTo(INSERTED);
        DatabaseSaveResult<UUID> second = multikeyTable.newSaveBuilderWithUUID("id", null)
                .uniqueKey("first_name", "John")
                .uniqueKey("last_name", "Smith")
                .setField("address", "Java St 1")
                .execute(connection);
        assertThat(second.getSaveStatus()).isEqualTo(INSERTED);
        assertThat(first.getId()).isNotEqualTo(second.getId());
        DatabaseSaveResult<UUID> third = multikeyTable.newSaveBuilderWithUUID("id", null)
                .uniqueKey("first_name", "John")
                .uniqueKey("last_name", "Smith")
                .setField("address", "Updated St 1")
                .execute(connection);
        assertThat(third.getSaveStatus()).isEqualTo(UPDATED);
        assertThat(second.getId()).isEqualTo(third.getId());
        assertThat(multikeyTable.where("id", second.getId()).singleString(connection, "address").get())
                .isEqualTo("Updated St 1");
    }

}
