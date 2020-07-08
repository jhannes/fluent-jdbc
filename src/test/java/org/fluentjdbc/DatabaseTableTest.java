package org.fluentjdbc;

import org.fluentjdbc.DatabaseTable.RowMapper;
import org.fluentjdbc.h2.H2TestDatabase;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.fluentjdbc.FluentJdbcAsserts.assertThat;

public class DatabaseTableTest extends AbstractDatabaseTest {

    private final DatabaseTable table = new DatabaseTableImpl("database_table_test_table");

    protected final Connection connection;

    private final DatabaseTable missingTable = new DatabaseTableImpl("non_existing");

    public DatabaseTableTest() throws SQLException {
        this(H2TestDatabase.createConnection(), H2TestDatabase.REPLACEMENTS);
    }

    protected DatabaseTableTest(Connection connection, Map<String, String> replacements) {
        super(replacements);
        this.connection = connection;
    }

    @Before
    public void createTable() throws SQLException {
        dropTableIfExists(connection, "database_table_test_table");
        try(Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(preprocessCreateTable("create table database_table_test_table (id ${INTEGER_PK}, code integer not null, name varchar(50) not null, description varchar(100) null)"));
        }
    }

    @Test
    public void shouldInsertWithoutKey() throws SQLException {
        table.insert()
            .setField("code", 1001)
            .setField("name", "insertTest")
            .execute(connection);

        Object id = table.insert()
            .setPrimaryKey("id", null)
            .setField("code", 1002)
            .setField("name", "insertTest")
            .execute(connection);
        assertThat(id).isNotNull();

        assertThat(table.where("name", "insertTest").orderBy("code").listLongs(connection, "code"))
            .containsExactly(1001L, 1002L);
    }



    @Test
    public void shouldHandleOrStatements() throws SQLException {
        Object id1 = table.insert().setPrimaryKey("id", null).setField("code", 1001).setField("name", "A").execute(connection);
        Object id2 = table.insert().setPrimaryKey("id", null).setField("code", 1002).setField("name", "B").execute(connection);
        Object id3 = table.insert().setPrimaryKey("id", null).setField("code", 2001).setField("name", "C").execute(connection);
        Object id4 = table.insert().setPrimaryKey("id", null).setField("code", 2002).setField("name", "D").execute(connection);

        assertThat(table
                .whereExpressionWithMultipleParameters("(name = ? OR name = ? OR name = ?)", Arrays.asList("A","B", "C"))
                .whereExpressionWithMultipleParameters("(name = ? OR code > ?)", Arrays.asList("A", 2000L))
                .unordered()
                .listStrings(connection, "id"))
                .containsOnly(id1.toString(), id3.toString())
                .doesNotContain(id2.toString(), id4.toString());
    }

    @Test
    public void shouldListOnWhereIn() throws SQLException {
        Object id1 = table.insert().setPrimaryKey("id", null).setField("code", 1).setField("name", "hello").execute(connection);
        Object id2 = table.insert().setPrimaryKey("id", null).setField("code", 2).setField("name", "world").execute(connection);
        Object id3 = table.insert().setPrimaryKey("id", null).setField("code", 3).setField("name", "darkness").execute(connection);

        assertThat(table.whereIn("name", Arrays.asList("hello", "world")).unordered().listStrings(connection, "id"))
            .containsOnly(id1.toString(), id2.toString())
            .doesNotContain(id3.toString());
    }

    @Test
    public void shouldReturnEmptyListOnEmptyWhereIn() throws SQLException {
        table.insert().setPrimaryKey("id", null).setField("code", 1).setField("name", "hello").execute(connection);
        assertThat(table.whereIn("name", Collections.emptyList()).unordered().listStrings(connection, "id"))
                .isEmpty();

    }

    @Test
    public void shouldListOnOptional() throws SQLException {
        Object id1 = table.insert().setPrimaryKey("id", null).setField("code", 1).setField("name", "yes").execute(connection);
        Object id2 = table.insert().setPrimaryKey("id", null).setField("code", 2).setField("name", "yes").execute(connection);
        Object id3 = table.insert().setPrimaryKey("id", null).setField("code", 3).setField("name", "no").execute(connection);

        assertThat(table.whereOptional("name", "yes").unordered().listStrings(connection, "id"))
            .contains(id1.toString(), id2.toString()).doesNotContain(id3.toString());
        assertThat(table.whereOptional("name", null).unordered().listStrings(connection, "id"))
            .contains(id1.toString(), id2.toString(), id3.toString());
    }


    @Test
    public void shouldInsertWithExplicitKey() throws SQLException {
        Object id = table.insert()
                .setPrimaryKey("id", 453534643)
                .setField("code", 1003)
                .setField("name", "insertTest")
                .execute(connection);

        assertThat(id).isEqualTo(453534643);
    }

    @Test
    public void shouldUpdate() throws SQLException {
        Object id = table.insert()
                .setPrimaryKey("id", null)
                .setField("code", 1004)
                .setField("name", "oldName")
                .execute(connection);

        table.where("id", id).query().update().setField("name", "New name").execute(connection);

        assertThat(table.where("id", id).singleString(connection, "name")).get()
            .isEqualTo("New name");
    }

    @Test
    public void shouldUpdateIfPresent() throws SQLException {
        Object id = table.insert()
                .setPrimaryKey("id", null)
                .setField("code", 1004)
                .setField("name", "oldName")
                .setField("description", "oldComment")
                .execute(connection);

        table.where("id", id).update().setFieldIfPresent("name", null).execute(connection);

        table.where("id", id).update()
                .setFieldIfPresent("name", null)
                .setFieldIfPresent("description", "newComment")
                .execute(connection);

        assertThat(table.where("id", id).singleString(connection, "name")).get().isEqualTo("oldName");
        assertThat(table.where("id", id).singleString(connection, "description")).get().isEqualTo("newComment");
    }

    @Test
    public void shouldSpecifyCustomExpressions() throws SQLException {
        Long id = table.insert().setPrimaryKey("id", (Long)null)
                .setField("code", 2)
                .setField("name", "test")
                .setField("description", null)
                .execute(connection);

        assertThat(table.whereExpression("description is null").unordered().listLongs(connection, "id"))
                .contains(id);
    }

    @Test
    public void shouldDelete() throws SQLException {
        Long id = (Long) table.insert().setPrimaryKey("id", null).setField("code", 1).setField("name", "hello").execute(connection);

        table.where("name", "hello").delete(connection);
        assertThat(table.unordered().listLongs(connection, "id"))
            .doesNotContain(id);
    }


    @Test
    public void shouldThrowOnMissingColumn() throws Exception {
        final Object id = table.insert()
                .setPrimaryKey("id", null)
                .setField("code", 1234)
                .setField("name", "testing")
                .execute(connection);

        assertThatThrownBy(() -> table.where("id", id).singleString(connection, "non_existing"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Column {non_existing} is not present");
    }

    @Test
    public void shouldThrowOnMissingTable() {
        assertThatThrownBy(() -> missingTable.where("id", 12).singleLong(connection, "id"))
                .isInstanceOf(SQLException.class);

        assertThatThrownBy(() -> missingTable.where("id", 12).unordered()
                .list(connection, (RowMapper<Object>) row -> row.getLong("id")))
                .isInstanceOf(SQLException.class);

        assertThatThrownBy(() -> missingTable.insert().setField("id", 12).execute(connection))
                .isInstanceOf(SQLException.class);

        assertThatThrownBy(() -> missingTable.insert().setField("name", "abc").execute(connection))
                .isInstanceOf(SQLException.class);
    }

    @Test
    public void shouldThrowIfSingleQueryReturnsMultipleRows() {
        table.insert().setField("code", 123).setField("name", "the same name").execute(connection);
        table.insert().setField("code", 456).setField("name", "the same name").execute(connection);

        assertThatThrownBy(() -> table.where("name", "the same name").singleLong(connection, "code"))
                .isInstanceOf(IllegalStateException.class);
    }

}
