package org.fluentjdbc;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Java6Assertions.assertThat;

import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.fluentjdbc.h2.H2TestDatabase;
import org.fluentjdbc.opt.junit.DbContextRule;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Map;
import java.util.regex.Pattern;


public class DbContextTest {

    private final DataSource dataSource = createDataSource();

    protected JdbcDataSource createDataSource() {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setUrl("jdbc:h2:mem:dbcontext;DB_CLOSE_DELAY=-1");
        return dataSource;
    }

    @Rule
    public final DbContextRule dbContext = new DbContextRule(dataSource);

    private DbTableContext tableContext;

    private Map<String, String> replacements = H2TestDatabase.REPLACEMENTS;

    public DbContextTest() {
        this.tableContext = dbContext.table("database_table_test_table");
    }

    protected String preprocessCreateTable(String createTableStatement) {
        return createTableStatement
                .replaceAll(Pattern.quote("${UUID}"), replacements.get("UUID"))
                .replaceAll(Pattern.quote("${INTEGER_PK}"), replacements.get("INTEGER_PK"))
                .replaceAll(Pattern.quote("${DATETIME}"), replacements.get("DATETIME"))
                ;
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
            dropTableIfExists(connection, "database_table_test_table");
            try(Statement stmt = connection.createStatement()) {
                stmt.executeUpdate(preprocessCreateTable("create table database_table_test_table (id ${INTEGER_PK}, code integer not null, name varchar(50) not null)"));
            }
        }
    }

    @Test
    public void shouldInsertWithoutKey() {
        tableContext.insert()
            .setField("code", 1001)
            .setField("name", "insertTest")
            .execute();

        Object id = tableContext.insert()
            .setPrimaryKey("id", null)
            .setField("code", 1002)
            .setField("name", "insertTest")
            .execute();
        assertThat(id).isNotNull();

        assertThat(tableContext.where("name", "insertTest").orderBy("code").listLongs("code"))
            .containsExactly(1001L, 1002L);
    }

    @Test
    public void shouldListOnWhereIn() {
        Object id1 = tableContext.insert().setPrimaryKey("id", null).setField("code", 1).setField("name", "hello").execute();
        Object id2 = tableContext.insert().setPrimaryKey("id", null).setField("code", 2).setField("name", "world").execute();
        Object id3 = tableContext.insert().setPrimaryKey("id", null).setField("code", 3).setField("name", "darkness").execute();

        assertThat(tableContext.whereIn("name", Arrays.asList("hello", "world")).unordered().listStrings("id"))
            .containsOnly(id1.toString(), id2.toString())
            .doesNotContain(id3.toString());
    }

    @Test
    public void shouldListOnOptional() {
        Object id1 = tableContext.insert().setPrimaryKey("id", null).setField("code", 1).setField("name", "yes").execute();
        Object id2 = tableContext.insert().setPrimaryKey("id", null).setField("code", 2).setField("name", "yes").execute();
        Object id3 = tableContext.insert().setPrimaryKey("id", null).setField("code", 3).setField("name", "no").execute();

        assertThat(tableContext.whereOptional("name", "yes").unordered().listStrings("id"))
            .contains(id1.toString(), id2.toString()).doesNotContain(id3.toString());
        assertThat(tableContext.whereOptional("name", null).unordered().listStrings("id"))
            .contains(id1.toString(), id2.toString(), id3.toString());
    }


    @Test
    public void shouldInsertWithExplicitKey() {
        Object id = tableContext.insert()
                .setPrimaryKey("id", 453534643)
                .setField("code", 1003)
                .setField("name", "insertTest")
                .execute();

        assertThat(id).isEqualTo(453534643);
    }

    @Test
    public void shouldUpdate() {
        Object id = tableContext.insert()
                .setPrimaryKey("id", null)
                .setField("code", 1004)
                .setField("name", "oldName")
                .execute();

        tableContext.where("id", id).update().setField("name", "New name").execute();

        assertThat(tableContext.where("id", id).singleString("name"))
            .isEqualTo("New name");
    }

    @Test
    public void shouldDelete() {
        Long id = (Long) tableContext.insert()
                .setPrimaryKey("id", null)
                .setField("code", 1)
                .setField("name", "hello")
                .execute();

        tableContext.where("name", "hello").executeDelete();
        assertThat(tableContext.listObjects(row -> row.getLong("id")))
            .doesNotContain(id);
    }


    @Test
    public void shouldThrowOnMissingColumn() {
        final Object id = tableContext.insert()
                .setPrimaryKey("id", null)
                .setField("code", 1234)
                .setField("name", "testing")
                .execute();

        assertThatThrownBy(new ThrowingCallable() {
            @Override
            public void call() {
                tableContext.where("id", id).singleString("non_existing");
            }
        })
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Column {non_existing} is not present");
    }

    @Test
    public void shouldThrowIfSingleQueryReturnsMultipleRows() {
        tableContext.insert().setField("code", 123).setField("name", "the same name").execute();
        tableContext.insert().setField("code", 456).setField("name", "the same name").execute();

        assertThatThrownBy(new ThrowingCallable() {
            @Override
            public void call() {
                tableContext.where("name", "the same name").singleLong("code");
            }
        }).isInstanceOf(IllegalStateException.class);

    }

}
