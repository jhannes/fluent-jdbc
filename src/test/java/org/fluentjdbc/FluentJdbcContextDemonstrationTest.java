package org.fluentjdbc;

import org.fluentjdbc.h2.H2TestDatabase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class FluentJdbcContextDemonstrationTest {

    private final DbContext dbContext;
    private final DbContextTable table;
    private final DbContextTable tableWithStringKeyContext;
    private final Map<String, String> replacements = H2TestDatabase.REPLACEMENTS;
    private final DataSource dataSource;

    private DbContextConnection connection;

    public FluentJdbcContextDemonstrationTest() {
        dataSource = H2TestDatabase.createDataSource();
        this.dbContext = new DbContext();
        this.table = dbContext.tableWithTimestamps("demo_table");
        this.tableWithStringKeyContext = dbContext.tableWithTimestamps("demo_string_table");
    }

    protected String preprocessCreateTable(String createTableStatement) {
        return AbstractDatabaseTest.preprocessCreateTable(createTableStatement, replacements);
    }

    protected void dropTableIfExists(Connection connection, String tableName) {
        try(Statement stmt = connection.createStatement()) {
            stmt.executeUpdate("drop table " + tableName);
        } catch(SQLException ignored) {
        }
    }


    @Before
    public void createTable() throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            dropTableIfExists(connection, "demo_table");
            dropTableIfExists(connection, "demo_string_table");
            try(Statement stmt = connection.createStatement()) {
                stmt.executeUpdate(preprocessCreateTable("create table demo_table (id ${INTEGER_PK}, code integer not null, name varchar(50) not null, updated_at ${DATETIME} not null, created_at ${DATETIME} not null)"));
                stmt.executeUpdate(preprocessCreateTable("create table demo_string_table (id varchar(200) not null primary key, value integer not null, updated_at ${DATETIME} not null, created_at ${DATETIME} not null)"));
            }
        }

        connection = dbContext.startConnection(dataSource);
    }

    @After
    public void closeConnection() {
        connection.close();
    }

    @Test
    public void shouldGenerateIdForNewRow() {
        String savedName = "demo row";
        Long id = table
                .newSaveBuilder("id", null)
                .uniqueKey("code", 123)
                .setField("name", savedName)
                .execute()
                .getId();

        assertThat(table.where("id", id).singleString("name")).get().isEqualTo(savedName);
    }

    @Test
    public void shouldUpdateRowWithExistingId() {
        String savedName = "demo row";
        Long id = table
                .newSaveBuilder("id", null)
                .uniqueKey("code", 123)
                .setField("name", savedName)
                .execute()
                .getId();
        String updatedName = "updated name";
        table
                .newSaveBuilder("id", id)
                .uniqueKey("code", 543)
                .setField("name", updatedName)
                .execute();

        assertThat(table.where("id", id).singleString("name")).get().isEqualTo(updatedName);
        assertThat(table.where("id", id).singleLong("code")).get().isEqualTo(543L);
    }

    @Test
    public void shouldInsertWithStringPrimaryKey() {
        tableWithStringKeyContext.newSaveBuilderWithString("id", "abc")
                .setField("value", 1234L)
                .execute();

        assertThat(tableWithStringKeyContext.where("id", "abc").listLongs("value"))
                .containsOnly(1234L);
    }

    @Test
    public void shouldUpdateWithStringPrimaryKey() {
        tableWithStringKeyContext.newSaveBuilderWithString("id", "pqr")
                .setField("value", 1234L)
                .execute();
        tableWithStringKeyContext.newSaveBuilderWithString("id", "pqr")
                .setField("value", 9999L)
                .execute();

        assertThat(tableWithStringKeyContext.where("id", "pqr").listLongs("value"))
                .containsOnly(9999L);
    }

    @Test
    public void shouldRejectSaveWithStringPrimaryKeyNull() {
        String id = null;
        DbContextSaveBuilder<String> builder = tableWithStringKeyContext
                .newSaveBuilderWithString("id", id)
                .setField("value", 1234L);
        assertThatThrownBy(builder::execute)
            .isInstanceOf(SQLException.class)
            .hasMessageContaining("NULL").hasMessageContaining("ID");
    }

    @Test
    public void shouldInsertRowWithNonexistentKey() {
        String newRow = "Nonexistent key";
        long pregeneratedId = 1000 + new Random().nextInt();
        Long id = table.newSaveBuilder("id", pregeneratedId)
                .uniqueKey("code", 235235)
                .setField("name", newRow)
                .execute()
                .getId();

        assertThat(table.where("id", id).singleString("name")).get()
            .isEqualTo(newRow);
    }

    @Test
    public void shouldUpdateRowWithDuplicateUniqueKey() {
        String savedName = "old value";
        Long id = table.newSaveBuilder("id", null)
                .uniqueKey("code", 242112)
                .setField("name", savedName)
                .execute()
                .getId();
        String updatedName = "updated name";
        table.newSaveBuilder("id", null)
                .uniqueKey("code", 242112)
                .setField("name", updatedName)
                .execute();

        assertThat(table.where("id", id).singleString("name")).get()
            .isEqualTo(updatedName);
    }

    @Test
    public void shouldCreateTimestamps() throws InterruptedException {
        Instant start = Instant.now();
        Thread.sleep(10);
        Long id = table
                .newSaveBuilder("id", null)
                .uniqueKey("code", 32352)
                .setField("name", "demo row")
                .execute()
                .getId();
        Thread.sleep(10);

        assertThat(table.where("id", id).singleInstant("created_at").orElseThrow(IllegalArgumentException::new))
            .isAfter(start).isBefore(Instant.now());
        assertThat(table.where("id", id).singleInstant("updated_at").orElseThrow(IllegalArgumentException::new))
            .isAfter(start).isBefore(Instant.now());
    }

    @Test
    public void shouldUpdateTimestamp() throws InterruptedException {
        Long id = table
                .newSaveBuilder("id", null)
                .uniqueKey("code", 32352)
                .setField("name", "demo row")
                .execute()
                .getId();
        Instant createdTime = table.where("id", id).singleInstant("updated_at").orElseThrow(IllegalArgumentException::new);
        Instant updatedTime = table.where("id", id).singleInstant("updated_at").orElseThrow(IllegalArgumentException::new);
        Thread.sleep(10);

        table.newSaveBuilder("id", id).setField("name", "another value").execute();
        assertThat(table.where("id", id).singleInstant("updated_at").orElseThrow(IllegalArgumentException::new))
                .isAfter(updatedTime);
        assertThat(table.where("id", id).singleObject(row -> row.getOffsetDateTime("created_at")).orElseThrow(IllegalArgumentException::new))
                .isEqualTo(OffsetDateTime.ofInstant(createdTime, ZoneId.systemDefault()));
    }

    @Test
    public void shouldNotUpdateUnchangedRows() throws InterruptedException {
        Long id = table
                .newSaveBuilder("id", null)
                .uniqueKey("code", 32352)
                .setField("name", "original value")
                .execute()
                .getId();
        Instant updatedTime = table.where("id", id).singleInstant("updated_at").orElseThrow(IllegalArgumentException::new);
        Thread.sleep(10);

        table.newSaveBuilder("id", id).setField("name", "original value").execute();
        assertThat(table.where("id", id).singleInstant("updated_at")).get()
            .isEqualTo(updatedTime);
    }

}
