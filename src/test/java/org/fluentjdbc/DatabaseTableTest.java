package org.fluentjdbc;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.fluentjdbc.DatabaseTable.RowMapper;
import org.fluentjdbc.h2.H2TestDatabase;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import static org.fluentjdbc.FluentJdbcAsserts.assertThat;

public class DatabaseTableTest extends AbstractDatabaseTest {

    private DatabaseTable table = new DatabaseTableImpl("database_table_test_table");

    private Connection connection;

    private DatabaseTable missingTable = new DatabaseTableImpl("non_existing");

    public DatabaseTableTest() throws SQLException {
        this(H2TestDatabase.createConnection());
    }

    protected DatabaseTableTest(Connection connection) {
        this.connection = connection;
    }

    @Before
    public void createTable() throws SQLException {
        try(Statement stmt = connection.createStatement()) {
            stmt.executeUpdate("drop table if exists database_table_test_table");
            stmt.executeUpdate(preprocessCreateTable(connection,
                    "create table database_table_test_table (id integer primary key auto_increment, code integer not null, name varchar not null)"));
        }
    }

    @Test
    public void shouldInsertWithoutAndWithoutKey() {
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

        Object id2 = table.insert()
                .setPrimaryKey("id", 453534643)
                .setField("code", 1003)
                .setField("name", "insertTest")
                .execute(connection);

        assertThat(id2).isEqualTo(453534643);

        assertThat(table.where("name", "insertTest").listLongs(connection, "code"))
            .contains(1001L, 1002L, 1003L);
    }


    @Test
    public void shouldThrowOnMissingColumn() throws Exception {
        final Object id = table.insert()
                .setPrimaryKey("id", null)
                .setField("code", 1234)
                .setField("name", "testing")
                .execute(connection);

        assertThatThrownBy(new ThrowingCallable() {
            @Override
            public void call() throws Throwable {
                table.where("id", id).singleString(connection, "non_existing");
            }
        })
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Column {non_existing} is not present");
    }

    @Test
    public void shouldThrowOnMissingTable() {
        assertThatThrownBy(new ThrowingCallable() {
            @Override
            public void call() throws Throwable {
                missingTable.where("id", 12).singleLong(connection, "id");
            }
        }).isInstanceOf(SQLException.class);

        assertThatThrownBy(new ThrowingCallable() {
            @Override
            public void call() throws Throwable {
                missingTable.where("id", 12).list(connection, new RowMapper<Object>() {
                    @Override
                    public Object mapRow(DatabaseRow row) throws SQLException {
                        return row.getLong("id");
                    }
                });
            }
        }).isInstanceOf(SQLException.class);

        assertThatThrownBy(new ThrowingCallable() {
            @Override
            public void call() throws Throwable {
                missingTable.insert().setField("id", 12).execute(connection);
            }
        }).isInstanceOf(SQLException.class);

        assertThatThrownBy(new ThrowingCallable() {
            @Override
            public void call() throws Throwable {
                missingTable.insert().setField("name", "abc").execute(connection);
            }
        }).isInstanceOf(SQLException.class);
    }

    @Test
    public void shouldThrowIfSingleQueryReturnsMultipleRows() {
        table.insert().setField("code", 123).setField("name", "the same name").execute(connection);
        table.insert().setField("code", 456).setField("name", "the same name").execute(connection);

        assertThatThrownBy(new ThrowingCallable() {
            @Override
            public void call() throws Throwable {
                table.where("name", "the same name").singleLong(connection, "code");
            }
        })
            .isInstanceOf(IllegalStateException.class);

    }

}
