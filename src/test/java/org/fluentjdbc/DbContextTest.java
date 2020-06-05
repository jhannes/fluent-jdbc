package org.fluentjdbc;

import org.fluentjdbc.h2.H2TestDatabase;
import org.fluentjdbc.opt.junit.DbContextRule;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Map;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.assertThat;


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
    public void shouldHandleOrStatements() {
        tableContext.insert()
                .setFields(Arrays.asList("code", "name"), Arrays.asList(1001, "A"))
                .execute();
        tableContext.insert()
                .setField("code", 1002)
                .setField("name", "B")
                .execute();
        tableContext.insert()
                .setField("code", 2001)
                .setField("name", "C")
                .execute();
        tableContext.insert()
                .setField("code", 2002)
                .setField("name", "D")
                .execute();

        assertThat(tableContext
                .whereExpressionWithMultipleParameters("(name = ? OR name = ? OR name = ?)", Arrays.asList("A","B", "C"))
                .whereExpressionWithMultipleParameters("(name = ? OR code > ?)", Arrays.asList("A", 2000L))
                .unordered()
                .listLongs("code"))
                .containsExactlyInAnyOrder(1001L, 2001L);
    }

    @Test
    public void shouldHaveAccessToConnection() throws SQLException {
        tableContext.insert()
                .setField("code", 1001)
                .setField("name", "customSqlTest")
                .execute();

        String customSql = String.format("select code from %s where name = 'customSqlTest'", tableContext.getTable().getTableName());
        ResultSet resultSet = dbContext.getThreadConnection()
                .prepareStatement(customSql)
                .executeQuery();
        resultSet.next();

        assertThat(resultSet.getLong("code")).isEqualTo(1001);
    }

    @Test
    public void shouldBeAbleToTurnOffAutoCommits() throws InterruptedException {
        final Thread thread = new Thread(() -> {
            try (DbContextConnection ignored = dbContext.startConnection(getConnectionWithoutAutoCommit())) {
                tableContext.insert()
                        .setField("code", 1001)
                        .setField("name", "insertTest")
                        .execute();
            }
        });
        thread.start();
        thread.join();

        assertThat(tableContext.where("name", "insertTest").unordered().listLongs("code"))
                .isEmpty();
    }

    @Test
    public void shouldBeAbleToManuallyCommit() throws InterruptedException {
        final Thread thread = new Thread(() -> {
            try (DbContextConnection dbContextConnection = dbContext.startConnection(getConnectionWithoutAutoCommit())) {
                tableContext.insert()
                        .setField("code", 1001)
                        .setField("name", "insertTest")
                        .execute();
                dbContextConnection.commitTransaction();
            }
        });
        thread.start();
        thread.join();

        assertThat(tableContext.where("name", "insertTest").unordered().listLongs("code"))
                .containsExactly(1001L);
    }

    private ConnectionSupplier getConnectionWithoutAutoCommit() {
        return () -> {
            final Connection connection = dataSource.getConnection();
            connection.setAutoCommit(false);
            return connection;
        };
    }

    private Exception thrownException;

    @Test
    public void shouldThrowOnCommitWhenAutocommitIsTrue() throws InterruptedException {
        final Thread thread = new Thread(() -> {
            try (DbContextConnection dbContextConnection = dbContext.startConnection(getConnectionWithAutoCommit())) {
                tableContext.insert().setField("code", 1001).execute();
                dbContextConnection.commitTransaction();
            } catch (Exception e) {
                thrownException = e;
            }
        });
        thread.start();
        thread.join();

        assertThat(thrownException).isInstanceOf(SQLException.class);
    }

    private ConnectionSupplier getConnectionWithAutoCommit() {
        return () -> {
            final Connection connection = dataSource.getConnection();
            connection.setAutoCommit(true);
            return connection;
        };
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
    public void shouldCommitTransaction() {
        try (DbTransaction tx = dbContext.ensureTransaction()) {
            tableContext.insert().setField("code", 1003).setField("name", "commitTest").execute();
            tx.setComplete();
        }
        assertThat(tableContext.where("name", "commitTest").listLongs("code"))
                .contains(1003L);
    }

    @Test
    public void shouldThrowOnDoubleStart() {
        assertThatThrownBy(() -> dbContext.startConnection(dataSource))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Don't set twice in a thread!");
    }

    @Test
    public void shouldThrowIfContextIsNotStarted() throws InterruptedException {
        Thread thread = new Thread(() -> {
            try {
                dbContext.getThreadConnection();
            } catch (Exception e) {
                thrownException = e;
            }
        });
        thread.start();
        thread.join(5000);
        assertThat(thrownException)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Call startConnection");
    }

    @Test
    public void shouldSoftenException() throws SQLException {
        DbTransaction tx = dbContext.ensureTransaction();
        dbContext.getThreadConnection().close();
        assertThatThrownBy(tx::close)
                .isInstanceOf(SQLException.class);
    }

    @Test
    public void shouldRollbackTransaction() {
        try (DbTransaction tx = dbContext.ensureTransaction()) {
            tableContext.insert().setField("code", 1004).setField("name", "txTest").execute();
            tableContext.insert().setField("code", 1005).setField("name", "txTest").execute();
        }
        assertThat(tableContext.where("name", "txTest").listLongs("code")).isEmpty();
    }

    @Test
    public void shouldCommitNestedTransaction() {
        try (DbTransaction outerTx = dbContext.ensureTransaction()) {
            tableContext.insert().setField("code", 1006).setField("name", "nestedTx").execute();
            try (DbTransaction innerTx = dbContext.ensureTransaction()) {
                tableContext.insert().setField("code", 1007).setField("name", "nestedTx").execute();
                innerTx.setComplete();
            }
            outerTx.setComplete();
        }
        assertThat(tableContext.where("name", "nestedTx").listLongs("code"))
                .contains(1006L, 1006L);
    }

    @Test
    public void shouldRollbackNestedTransaction() {
        try (DbTransaction tx = dbContext.ensureTransaction()) {
            tableContext.insert().setField("code", 1008).setField("name", "nestedTx").execute();
            try (DbTransaction innerTx = dbContext.ensureTransaction()) {
                tableContext.insert().setField("code", 1009).setField("name", "nestedTx").execute();
                // Rollback by default
            }
            tx.setComplete();
        }
        assertThat(tableContext.where("name", "nestedTx").listLongs("code"))
                .contains();
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
    public void shouldCountRows() {
        tableContext.insert().setPrimaryKey("id", null).setField("code", 1).setField("name", "shouldCountRows").execute();
        tableContext.insert().setPrimaryKey("id", null).setField("code", 2).setField("name", "shouldCountRows").execute();
        tableContext.insert().setPrimaryKey("id", null).setField("code", 3).setField("name", "SomethingElse").execute();

        assertThat(tableContext.where("name", "shouldCountRows").getCount()).isEqualTo(2);
    }

    @Test
    public void orderRows() {
        tableContext.insert().setPrimaryKey("id", null).setField("code", 1).setField("name", "C").execute();
        tableContext.insert().setPrimaryKey("id", null).setField("code", 2).setField("name", "B").execute();
        tableContext.insert().setPrimaryKey("id", null).setField("code", 3).setField("name", "A").execute();

        assertThat(tableContext.orderedBy("name").listStrings("name"))
                .containsExactly("A", "B", "C");
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

        assertThat(tableContext.where("id", id).singleString("name")).get()
            .isEqualTo("New name");
    }

    @Test
    public void shouldReadFromCache() {
        Object id = tableContext.insert()
                .setPrimaryKey("id", null)
                .setField("code", 1005)
                .setField("name", "hello")
                .execute();

        assertThat(tableContext.cache(id,
                i -> tableContext.where("id", i).singleObject(row -> row.getString("name"))
        )).get().isEqualTo("hello");

        tableContext.where("id", id)
                .update()
                .setField("name", "updated")
                .execute();

        assertThat(tableContext.cache(id,
                i -> tableContext.where("id", i).singleObject(row -> row.getString("name"))
        )).get().isEqualTo("hello");
    }

    @Test
    public void shouldDelete() {
        Long id = (Long) tableContext.insert()
                .setPrimaryKey("id", null)
                .setField("code", 1)
                .setField("name", "hello")
                .execute();

        tableContext
                .whereAll(Arrays.asList("code", "name"), Arrays.asList(1, "hello"))
                .whereExpression("id is not null")
                .executeDelete();
        assertThat(tableContext.unordered().listLongs("id"))
            .doesNotContain(id);
    }

    @Test
    public void shouldThrowOnMissingColumn() {
        final Object id;
        id = tableContext.insert()
                .setPrimaryKey("id", null)
                .setField("code", 1234)
                .setField("name", "testing")
                .execute();

        assertThatThrownBy(() -> tableContext.where("id", id).singleString("non_existing"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Column {non_existing} is not present");
    }

    @Test
    public void shouldThrowIfSingleQueryReturnsMultipleRows() {
        tableContext.insert().setField("code", 123).setField("name", "the same name").execute();
        tableContext.insert().setField("code", 456).setField("name", "the same name").execute();

        assertThatThrownBy(() -> tableContext.where("name", "the same name").singleLong("code")).isInstanceOf(IllegalStateException.class);

    }

}
