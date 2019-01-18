package org.fluentjdbc;

import static org.assertj.core.api.Java6Assertions.assertThat;
import static org.fluentjdbc.FluentJdbcAsserts.assertThat;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.Map;
import java.util.Random;
import java.util.regex.Pattern;

import org.fluentjdbc.h2.H2TestDatabase;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class FluentJdbcContextDemonstrationTest {

    private DbContext dbContext;
    private DbTableContext tableContext;
    private Map<String, String> replacements = H2TestDatabase.REPLACEMENTS;
    private DbContextConnection connection;
    private JdbcDataSource dataSource = new JdbcDataSource();

    public FluentJdbcContextDemonstrationTest() throws SQLException {
        dataSource.setUrl("jdbc:h2:mem:dbcontext;DB_CLOSE_DELAY=-1");
        this.dbContext = new DbContext();
        this.tableContext = dbContext.tableWithTimestamps("demo_table");
    }

    protected String preprocessCreateTable(String createTableStatement) throws SQLException {
        return createTableStatement
                .replaceAll(Pattern.quote("${UUID}"), replacements.get("UUID"))
                .replaceAll(Pattern.quote("${INTEGER_PK}"), replacements.get("INTEGER_PK"))
                .replaceAll(Pattern.quote("${DATETIME}"), replacements.get("DATETIME"))
                ;
    }

    protected void dropTableIfExists(Connection connection, String tableName) throws SQLException {
        try(Statement stmt = connection.createStatement()) {
            stmt.executeUpdate("drop table " + tableName);
        } catch(SQLException e) {
        }
    }


    @Before
    public void createTable() throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            dropTableIfExists(connection, "demo_table");
            try(Statement stmt = connection.createStatement()) {
                stmt.executeUpdate(preprocessCreateTable("create table demo_table (id ${INTEGER_PK}, code integer not null, name varchar(50) not null, updated_at ${DATETIME} not null, created_at ${DATETIME} not null)"));
            }
        }

        connection = dbContext.startConnection(dataSource);
    }

    @After
    public void closeConnection() throws SQLException {
        connection.close();
    }

    @Test
    public void shouldGenerateIdForNewRow() {
        String savedName = "demo row";
        Long id = tableContext
                .newSaveBuilder("id", (Long)null)
                .uniqueKey("code", 123)
                .setField("name", savedName)
                .execute();

        String retrievedName = tableContext.where("id", id).singleString("name");
        assertThat(retrievedName).isEqualTo(savedName);
    }

    @Test
    public void shouldUpdateRowWithExistingId() {
        String savedName = "demo row";
        Long id = tableContext
                .newSaveBuilder("id", (Long)null)
                .uniqueKey("code", 123)
                .setField("name", savedName)
                .execute();
        String updatedName = "updated name";
        tableContext
                .newSaveBuilder("id", id)
                .uniqueKey("code", 543)
                .setField("name", updatedName)
                .execute();

        String retrievedName = tableContext.where("id", id).singleString("name");
        assertThat(retrievedName).isEqualTo(updatedName);
        assertThat(tableContext.where("id", id).singleLong("code")).isEqualTo(543L);
    }

    @Test
    public void shouldInsertRowWithNonexistantKey() throws SQLException {
        String newRow = "Nonexistingent key";
        long pregeneratedId = 1000 + new Random().nextInt();
        Long id = tableContext.newSaveBuilder("id", pregeneratedId)
                .uniqueKey("code", 235235)
                .setField("name", newRow)
                .execute();

        assertThat(tableContext.where("id", id).singleString("name"))
            .isEqualTo(newRow);
    }

    @Test
    public void shouldUpdateRowWithDuplicateUniqueKey() throws SQLException {
        String savedName = "old value";
        Long id = tableContext.newSaveBuilder("id", null)
                .uniqueKey("code", 242112)
                .setField("name", savedName)
                .execute();
        String updatedName = "updated name";
        tableContext.newSaveBuilder("id", null)
                .uniqueKey("code", 242112)
                .setField("name", updatedName)
                .execute();

        assertThat(tableContext.where("id", id).singleString("name"))
            .isEqualTo(updatedName);
    }

    @Test
    public void shouldCreateTimestamps() throws InterruptedException, SQLException {
        Instant start = Instant.now();
        Thread.sleep(10);
        Long id = tableContext
                .newSaveBuilder("id", (Long)null)
                .uniqueKey("code", 32352)
                .setField("name", "demo row")
                .execute();
        Thread.sleep(10);

        assertThat(tableContext.where("id", id).singleDateTime("created_at"))
            .isAfter(start).isBefore(Instant.now());
        assertThat(tableContext.where("id", id).singleDateTime("updated_at"))
            .isAfter(start).isBefore(Instant.now());
    }

    @Test
    public void shouldUpdateTimestamp() throws InterruptedException, SQLException {
        Long id = tableContext
                .newSaveBuilder("id", (Long)null)
                .uniqueKey("code", 32352)
                .setField("name", "demo row")
                .execute();
        Instant createdTime = tableContext.where("id", id).singleDateTime("updated_at");
        Instant updatedTime = tableContext.where("id", id).singleDateTime("updated_at");
        Thread.sleep(10);

        tableContext.newSaveBuilder("id", id).setField("name", "another value").execute();
        assertThat(tableContext.where("id", id).singleDateTime("updated_at"))
            .isAfter(updatedTime);
        assertThat(tableContext.where("id", id).singleDateTime("created_at"))
            .isEqualTo(createdTime);
    }

    @Test
    public void shouldNotUpdateUnchangedRows() throws InterruptedException, SQLException {
        Long id = tableContext
                .newSaveBuilder("id", (Long)null)
                .uniqueKey("code", 32352)
                .setField("name", "original value")
                .execute();
        Instant updatedTime = tableContext.where("id", id).singleDateTime("updated_at");
        Thread.sleep(10);

        tableContext.newSaveBuilder("id", id).setField("name", "original value").execute();
        assertThat(tableContext.where("id", id).singleDateTime("updated_at"))
            .isEqualTo(updatedTime);
    }

}
