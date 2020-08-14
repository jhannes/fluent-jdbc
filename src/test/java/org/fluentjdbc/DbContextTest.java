package org.fluentjdbc;

import org.fluentjdbc.h2.H2TestDatabase;
import org.fluentjdbc.opt.junit.DbContextRule;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;


public class DbContextTest {

    @Rule
    public final DbContextRule dbContext;

    private final DataSource dataSource;

    private final DbContextTable table;

    private final Map<String, String> replacements;

    public DbContextTest() {
        this(H2TestDatabase.createDataSource(), H2TestDatabase.REPLACEMENTS);
    }

    protected DbContextTest(DataSource dataSource, Map<String, String> replacements) {
        this.dbContext = new DbContextRule(dataSource);
        this.dataSource = dataSource;
        this.table = dbContext.table("database_table_test_table");
        this.replacements = replacements;
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
            dropTableIfExists(connection, "database_table_test_table");
            try(Statement stmt = connection.createStatement()) {
                stmt.executeUpdate(preprocessCreateTable("create table database_table_test_table (id ${INTEGER_PK}, code integer not null, name varchar(50) null)"));
            }
        }
    }

    @Test
    public void shouldHandleOrStatements() {
        table.insert()
                .setFields(Arrays.asList("code", "name"), Arrays.asList(1001, "A"))
                .execute();
        table.insert()
                .setField("code", 1002)
                .setField("name", "B")
                .execute();
        table.insert()
                .setField("code", 2001)
                .setField("name", "C")
                .execute();
        table.insert()
                .setField("code", 2002)
                .setField("name", "D")
                .execute();

        assertThat(table
                .whereExpressionWithMultipleParameters("(name = ? OR name = ? OR name = ?)", Arrays.asList("A","B", "C"))
                .whereExpressionWithMultipleParameters("(name = ? OR code > ?)", Arrays.asList("A", 2000L))
                .unordered()
                .listLongs("code"))
                .containsExactlyInAnyOrder(1001L, 2001L);
    }

    @Test
    public void shouldHaveAccessToConnection() throws SQLException {
        table.insert()
                .setField("code", 1001)
                .setField("name", "customSqlTest")
                .execute();

        String customSql = String.format("select code from %s where name = 'customSqlTest'", table.getTable().getTableName());
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
                table.insert()
                        .setField("code", 1001)
                        .setField("name", "insertTest")
                        .execute();
            }
        });
        thread.start();
        thread.join();

        assertThat(table.where("name", "insertTest").unordered().listLongs("code"))
                .isEmpty();
    }

    @Test
    public void shouldBeAbleToManuallyCommit() throws InterruptedException {
        final Thread thread = new Thread(() -> {
            try (DbContextConnection ignored = dbContext.startConnection(getConnectionWithoutAutoCommit())) {
                try (DbTransaction tx = dbContext.ensureTransaction()) {
                    table.insert()
                            .setField("code", 1001)
                            .setField("name", "insertTest")
                            .execute();
                    tx.setComplete();
                }
            }
        });
        thread.start();
        thread.join();

        assertThat(table.where("name", "insertTest").unordered().listLongs("code"))
                .containsExactly(1001L);
    }

    @Test
    public void shouldSeparateConnectionPerDbContext() {
        DbContext rolledBackContext = new DbContext();
        DbContext committedContext = new DbContext();
        try (DbContextConnection ignored = rolledBackContext.startConnection(dataSource)) {
            DbContextTable table = rolledBackContext.table("database_table_test_table");
            try (DbTransaction dbTransaction = rolledBackContext.ensureTransaction()) {
                table.insert().setField("code", 2000).execute();
                dbTransaction.setRollback();

                try (DbContextConnection ignored2 = committedContext.startConnection(dataSource)) {
                    DbContextTable committedTable = committedContext.table("database_table_test_table");
                    try (DbTransaction committedTransaction = rolledBackContext.ensureTransaction()) {
                        committedTable.insert().setField("code", 3000).execute();
                        committedTransaction.setComplete();
                    }
                }
            }
        }
        assertThat(table.query().listLongs("code"))
                .contains(3000L)
                .doesNotContain(2000L);
    }
    
    @Test
    public void shouldNestContexts() {
        try (DbContextConnection ignored = dbContext.startConnection(dataSource)) {
            table.insert().setField("code", 1).execute();
            try (DbContextConnection ignored2 = dbContext.startConnection(dataSource)) {
                table.insert().setField("code", 2).execute();
            }
            table.insert().setField("code", 3).execute();
        }

        assertThat(table.query().listLongs("code")).contains(1L, 2L, 3L);
    }
    

    private DbContext.ConnectionSupplier getConnectionWithoutAutoCommit() {
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
            try (DbContextConnection ignore = dbContext.startConnection(getConnectionWithAutoCommit())) {
                try (DbTransaction tx = dbContext.ensureTransaction()) {
                    table.insert().setField("name", "test").execute();
                    tx.setComplete();
                }
            } catch (Exception e) {
                thrownException = e;
            }
        });
        thread.start();
        thread.join();

        assertThat(thrownException).isInstanceOf(SQLException.class);
    }

    private DbContext.ConnectionSupplier getConnectionWithAutoCommit() {
        return () -> {
            final Connection connection = dataSource.getConnection();
            connection.setAutoCommit(true);
            return connection;
        };
    }

    @Test
    public void shouldInsertWithoutKey() {
        table.insert()
            .setField("code", 1001)
            .setField("name", "insertTest")
            .execute();

        Object id = table.insert()
            .setPrimaryKey("id", null)
            .setField("code", 1002)
            .setField("name", "insertTest")
            .execute();
        assertThat(id).isNotNull();

        assertThat(table.where("name", "insertTest").orderBy("code").listLongs("code"))
            .containsExactly(1001L, 1002L);
    }

    @Test
    public void shouldCommitTransaction() {
        try (DbTransaction tx = dbContext.ensureTransaction()) {
            table.insert().setField("code", 1003).setField("name", "commitTest").execute();
            tx.setComplete();
        }
        assertThat(table.where("name", "commitTest").listLongs("code"))
                .contains(1003L);
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
            table.insert().setField("code", 1004).setField("name", "txTest").execute();
            table.insert().setField("code", 1005).setField("name", "txTest").execute();
            tx.setRollback();
        }
        assertThat(table.where("name", "txTest").listLongs("code")).isEmpty();
    }

    @Test
    public void shouldCommitNestedTransaction() {
        try (DbTransaction outerTx = dbContext.ensureTransaction()) {
            table.insert().setField("code", 1006).setField("name", "nestedTx").execute();
            try (DbTransaction innerTx = dbContext.ensureTransaction()) {
                table.insert().setField("code", 1007).setField("name", "nestedTx").execute();
                innerTx.setComplete();
            }
            outerTx.setComplete();
        }
        assertThat(table.where("name", "nestedTx").listLongs("code"))
                .contains(1006L, 1006L);
    }

    @Test
    public void shouldRollbackNestedTransaction() {
        try (DbTransaction tx = dbContext.ensureTransaction()) {
            table.insert().setField("code", 1008).setField("name", "nestedTx").execute();
            try (DbTransaction innerTx = dbContext.ensureTransaction()) {
                table.insert().setField("code", 1009).setField("name", "nestedTx").execute();
                innerTx.setRollback();
            }
            tx.setComplete();
        }
        assertThat(table.where("name", "nestedTx").listLongs("code"))
                .contains();
    }

    @Test
    public void shouldListOnWhereIn() {
        Object id1 = table.insert().setPrimaryKey("id", null).setField("code", 1).setField("name", "hello").execute();
        Object id2 = table.insert().setPrimaryKey("id", null).setField("code", 2).setField("name", "world").execute();
        Object id3 = table.insert().setPrimaryKey("id", null).setField("code", 3).setField("name", "darkness").execute();

        assertThat(table.whereIn("name", Arrays.asList("hello", "world")).unordered().listStrings("id"))
            .containsOnly(id1.toString(), id2.toString())
            .doesNotContain(id3.toString());
    }

    @Test
    public void shouldListOnOptional() {
        Object id1 = table.insert().setPrimaryKey("id", null).setField("code", 1).setField("name", "yes").execute();
        Object id2 = table.insert().setPrimaryKey("id", null).setField("code", 2).setField("name", "yes").execute();
        Object id3 = table.insert().setPrimaryKey("id", null).setField("code", 3).setField("name", "no").execute();

        assertThat(table.whereOptional("name", "yes").query().unordered().listStrings("id"))
            .contains(id1.toString(), id2.toString()).doesNotContain(id3.toString());
        assertThat(table.whereOptional("name", null).unordered().listStrings("id"))
            .contains(id1.toString(), id2.toString(), id3.toString());
    }

    @Test
    public void shouldCountRows() {
        table.insert().setPrimaryKey("id", null).setField("code", 1).setField("name", "shouldCountRows").execute();
        table.insert().setPrimaryKey("id", null).setField("code", 2).setField("name", "shouldCountRows").execute();
        table.insert().setPrimaryKey("id", null).setField("code", 3).setField("name", "SomethingElse").execute();

        assertThat(table.where("name", "shouldCountRows").getCount()).isEqualTo(2);
    }

    @Test
    public void orderRows() {
        table.insert().setPrimaryKey("id", null).setField("code", 1).setField("name", "C").execute();
        table.insert().setPrimaryKey("id", null).setField("code", 2).setField("name", "B").execute();
        table.insert().setPrimaryKey("id", null).setField("code", 3).setField("name", "A").execute();

        assertThat(table.orderedBy("name").listStrings("name"))
                .containsExactly("A", "B", "C");
    }

    @Test
    public void shouldLimitRows() {
        table.whereExpression("1 = 1").executeDelete();
        table.insert().setPrimaryKey("id", null).setField("code", 1).setField("name", "C").execute();
        table.insert().setPrimaryKey("id", null).setField("code", 2).setField("name", "B").execute();
        table.insert().setPrimaryKey("id", null).setField("code", 3).setField("name", "A").execute();
        table.insert().setPrimaryKey("id", null).setField("code", 4).setField("name", "A").execute();

        assertThat(table.orderedBy("name, code").limit(3).listLongs("code"))
                .containsExactly(3L, 4L, 2L);
        assertThat(table.orderedBy("name, code").skipAndLimit(2, 3).listLongs("code"))
                .containsExactly(2L, 1L);
    }


    @Test
    public void shouldInsertWithExplicitKey() {
        Object id = table.insert()
                .setPrimaryKey("id", 453534643)
                .setField("code", 1003)
                .setField("name", "insertTest")
                .execute();

        assertThat(id).isEqualTo(453534643);
    }

    @Test
    public void shouldUpdate() {
        Object id = table.insert()
                .setPrimaryKey("id", null)
                .setField("code", 1004)
                .setField("name", "oldName")
                .execute();

        table.where("id", id).update()
                .setFields(Arrays.asList("code"), Arrays.asList(1104))
                .setField("name", "New name").execute();

        assertThat(table.where("id", id).singleString("name")).get()
            .isEqualTo("New name");
        assertThat(table.where("id", id).singleLong("code")).get()
            .isEqualTo(1104L);
    }

    @Test
    public void shouldReadFromCache() {
        Object id = table.insert()
                .setPrimaryKey("id", null)
                .setField("code", 1005)
                .setField("name", "hello")
                .execute();

        assertThat(table.cache(id,
                i -> table.where("id", i).singleObject(row -> row.getString("name"))
        )).get().isEqualTo("hello");

        table.where("id", id)
                .update()
                .setField("name", "updated")
                .execute();

        assertThat(table.cache(id,
                i -> table.where("id", i).singleObject(row -> row.getString("name"))
        )).get().isEqualTo("hello");
    }

    @Test
    public void shouldDelete() {
        Long id = (Long) table.insert()
                .setPrimaryKey("id", null)
                .setField("code", 1)
                .setField("name", "hello")
                .execute();

        table
                .whereAll(Arrays.asList("code", "name"), Arrays.asList(1, "hello"))
                .whereExpression("id is not null")
                .executeDelete();
        assertThat(table.unordered().listLongs("id"))
            .doesNotContain(id);
        assertThat(table.query().where("id", id).singleLong("code")).isEmpty();
    }

    @Test
    public void shouldThrowOnMissingColumn() {
        final Object id;
        id = table.insert()
                .setPrimaryKey("id", null)
                .setField("code", 1234)
                .setField("name", "testing")
                .execute();

        assertThatThrownBy(() -> table.where("id", id).singleString("non_existing"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Column {non_existing} is not present");
    }

    @Test
    public void shouldThrowOnGetCountWithIllegalQuery() {
        assertThatThrownBy(() -> table.where("non_existing_column", "10").getCount())
                .isInstanceOf(SQLException.class);
    }

    @Test
    public void shouldThrowIfSingleQueryReturnsMultipleRows() {
        table.insert().setField("code", 123).setField("name", "the same name").execute();
        table.insert().setField("code", 456).setField("name", "the same name").execute();

        assertThatThrownBy(() -> table.where("name", "the same name").singleLong("code")).isInstanceOf(IllegalStateException.class);

    }

}
