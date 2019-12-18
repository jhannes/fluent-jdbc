package org.fluentjdbc.custom;

import org.fluentjdbc.AbstractDatabaseTest;
import org.fluentjdbc.DatabaseRow;
import org.fluentjdbc.DatabaseSaveBuilder;
import org.fluentjdbc.DatabaseSaveResult;
import org.fluentjdbc.DatabaseTable;
import org.fluentjdbc.DatabaseTableWithTimestamps;
import org.fluentjdbc.h2.H2TestDatabase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Java6Assertions.assertThat;
import static org.fluentjdbc.DatabaseSaveResult.SaveStatus.UPDATED;

public class CustomSaveBuilderTest extends AbstractDatabaseTest {
    private Connection connection;
    private DatabaseTable table = new DatabaseTableWithTimestamps("string_table");

    public CustomSaveBuilderTest() throws SQLException {
        this(createConnection(), H2TestDatabase.REPLACEMENTS);
    }

    protected CustomSaveBuilderTest(Connection connection, Map<String, String> replacements) {
        super(replacements);
        this.connection = connection;
    }

    private static Connection createConnection() throws SQLException {
        String jdbcUrl = System.getProperty("test.db.jdbc_url", "jdbc:h2:mem:DatabaseSaveBuilderTest");
        return DriverManager.getConnection(jdbcUrl);
    }

    @Before
    public void openConnection() throws SQLException {
        dropTableIfExists(connection, "string_table");
        try(Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(
                    preprocessCreateTable("create table string_table (id varchar(50) primary key, code integer not null, updated varchar(10), name varchar(50) not null, updated_at ${DATETIME} not null, created_at ${DATETIME} not null)"));
        }
    }

    @After
    public void closeConnection() throws SQLException {
        connection.close();
    }

    @Test
    public void canUseCustomSaveBuilder() throws Exception {
        String savedName = "original row";
        String id = new DatabaseSaveBuilderString(table, "id", null)
                .uniqueKey("code", 123)
                .setField("name", savedName)
                .execute(connection)
                .getId();

        DatabaseSaveResult<String> updateWithID = new DatabaseSaveBuilderString(table,"id", id)
                .uniqueKey("code", 543)
                .setField("name", "updated value")
                .execute(connection);
        assertThat(updateWithID.getSaveStatus()).isEqualTo(UPDATED);
        assertThat(table.where("id", id).singleString(connection, "name")).isEqualTo("updated value");

        DatabaseSaveResult<String> updateWithUniqueKey = new DatabaseSaveBuilderString(table,"id", null)
                .uniqueKey("code", 543)
                .setField("name", savedName)
                .execute(connection);
        assertThat(updateWithUniqueKey.getSaveStatus()).isEqualTo(UPDATED);
        assertThat(table.where("id", id).singleString(connection, "name")).isEqualTo(savedName);
    }

    @Test
    public void canHaveCustomUpdate() throws Exception {
        String savedName = "original row";
        String id = new DatabaseSaveBuilderString(table, "id", null)
                .uniqueKey("code", 123)
                .setField("name", savedName)
                .execute(connection)
                .getId();
        assertThat(table.where("id", id).singleString(connection, "updated")).isEqualTo("no");

        DatabaseSaveResult<String> updateWithID = new DatabaseSaveBuilderString(table,"id", id)
                .uniqueKey("code", 543)
                .setField("name", "updated value")
                .execute(connection);
        assertThat(updateWithID.getSaveStatus()).isEqualTo(UPDATED);
        assertThat(table.where("id", id).singleString(connection, "updated")).isEqualTo("yes");
    }

    class DatabaseSaveBuilderString extends DatabaseSaveBuilder<String> {
        protected DatabaseSaveBuilderString(DatabaseTable table, String idField, @Nullable String id) {
            super(table, idField, id);
        }

        @Nullable
        @Override
        protected String insert(Connection connection) {
            String idValue = this.idValue;
            if (idValue == null) {
                idValue = UUID.randomUUID().toString();
            }
            table.insert()
                    .setFields(fields, values)
                    .setField(idField, idValue)
                    .setFields(uniqueKeyFields, uniqueKeyValues)
                    .setField("updated", "no")
                    .execute(connection);
            return idValue;
        }

        @Override
        protected String update(Connection connection, String idValue) {
            table.where(idField, idValue)
                    .update()
                    .setFields(this.fields, this.values)
                    .setFields(uniqueKeyFields, uniqueKeyValues)
                    .setField("updated", "yes")
                    .execute(connection);
            return idValue;
        }

        @Override
        protected String getId(DatabaseRow row) throws SQLException {
            return row.getString(idField);
        }
    }
}
