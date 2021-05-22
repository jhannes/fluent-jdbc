package org.fluentjdbc;

import org.fluentjdbc.h2.H2TestDatabase;
import org.fluentjdbc.opt.junit.DbContextRule;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import javax.sql.DataSource;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.fluentjdbc.AbstractDatabaseTest.createTable;
import static org.fluentjdbc.AbstractDatabaseTest.dropTableIfExists;
import static org.fluentjdbc.AbstractDatabaseTest.getDatabaseProductName;


public class DbContextTest {

    @Rule
    public final DbContextRule dbContext;

    private final DataSource dataSource;

    private final DbContextTable table;

    private final Map<String, String> replacements;
    
    private boolean limitNotSupported = false;
    
    private boolean largeObjectsNotSupported = false;

    public DbContextTest() {
        this(H2TestDatabase.createDataSource(), H2TestDatabase.REPLACEMENTS);
    }

    protected DbContextTest(DataSource dataSource, Map<String, String> replacements) {
        this.dbContext = new DbContextRule(dataSource);
        this.dataSource = dataSource;
        this.table = dbContext.table("database_table_test_table");
        this.replacements = replacements;
    }
    
    protected void limitNotSupported() {
        this.limitNotSupported = true;
    }    

    protected void largeObjectsNotSupported() {
        this.largeObjectsNotSupported = true;
    }


    @Before
    public void setupDatabase() throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            dropTableIfExists(connection, "database_table_test_table");
            createTable(connection, "create table database_table_test_table (id ${INTEGER_PK}, code integer not null, name varchar(50) null, data ${BLOB}, document ${CLOB})", replacements);
        }
    }
    
    private void assumeLimitSupported() {
        Assume.assumeFalse("[" + getDatabaseProductName(dbContext.getThreadConnection()) + "] does not support limit", limitNotSupported);
    }

    private void assumeLargeObjectsSupported() {
        Assume.assumeFalse("[" + getDatabaseProductName(dbContext.getThreadConnection()) + "] does not support CLOB/BLOB", largeObjectsNotSupported);
    }

    @Test
    public void shouldHandleOrStatements() {
        insertTestRow(1001, "A");
        insertTestRow(1002, "B");
        insertTestRow(2001, "C");
        insertTestRow(2002, "D");

        assertThat(table
                .whereExpressionWithParameterList("name = ? OR name = ? OR name = ?", Arrays.asList("A","B", "C"))
                .whereExpressionWithParameterList("name = ? OR code > ?", Arrays.asList("A", 2000L))
                .unordered()
                .listLongs("code"))
                .containsExactlyInAnyOrder(1001L, 2001L);
    }

    @Test
    public void shouldHaveAccessToConnection() throws SQLException {
        insertTestRow(1001, "customSqlTest");

        String customSql = String.format("select code from %s where name = 'customSqlTest'", table.getTable().getTableName());
        ResultSet resultSet = dbContext.getThreadConnection()
                .prepareStatement(customSql)
                .executeQuery();
        resultSet.next();

        assertThat(resultSet.getLong("code")).isEqualTo(1001);
    }

    @Test
    public void shouldExecuteArbitrarySql() {
        insertTestRow(9000, "ZYX");
        insertTestRow(10000, "PQR");
        insertTestRow(10002, "ZYX");
        insertTestRow(10003, "ABC");

        assertThat(dbContext
                .statement("select max(code) as max_code from database_table_test_table", Arrays.asList())
                .singleObject(row -> row.getInt("max_code")))
                .get()
                .isEqualTo(10003);
        assertThat(dbContext
                .statement("select max(code) as max_code from database_table_test_table where name = ?", Arrays.asList("ZYX"))
                .list(row -> row.getInt("max_code")))
                .containsOnly(10002);
    }

    @Test
    public void shouldBuildSql() {
        insertTestRow(9000, "ZYX");
        insertTestRow(10000, "PQR");
        insertTestRow(10002, "ZYX");
        insertTestRow(10003, "ABC");

        DbContextSqlBuilder sqlBuilder = dbContext.select("max(code) as max_code")
                .from(table.getTable().getTableName())
                .where("name", "ZYX");
        assertThat(sqlBuilder.unordered().singleLong("max_code")).get().isEqualTo(10002L);
        assertThat(sqlBuilder.getCount()).isEqualTo(2);
    }

    @Test
    public void shouldBuildRowCount() {
        assumeLimitSupported();
        insertTestRow(9000, "ZYX");
        insertTestRow(10000, "PQR");
        insertTestRow(10002, "ZYX");
        insertTestRow(10003, "ABC");
        insertTestRow(10005, "ZYX");

        DbContextSqlBuilder sqlBuilder = dbContext.select("code")
                .query()
                .from(table.getTable().getTableName())
                .where("name", "ZYX")
                .orderBy("code");
        assertThat(sqlBuilder.getCount()).isEqualTo(3);
        assertThat(sqlBuilder.limit(2).stream(row -> row.getLong("code")))
                .containsExactly(9000L, 10002L);
    }

    @Test
    public void shouldBeAbleToTurnOffAutoCommits() throws InterruptedException {
        final Thread thread = new Thread(() -> {
            try (DbContextConnection ignored = dbContext.startConnection(getConnectionWithoutAutoCommit())) {
                insertTestRow(1001, "insertTest");
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
                //noinspection ResultOfMethodCallIgnored
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
        assumeLimitSupported();
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

        int count = table.where("id", id)
                .update()
                .setField("name", "updated")
                .execute();
        assertThat(count).isEqualTo(1);

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

        int count = table
                .whereAll(Arrays.asList("code", "name"), Arrays.asList(1, "hello"))
                .whereExpression("id is not null")
                .executeDelete();
        assertThat(count).isEqualTo(1);
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

        //noinspection ResultOfMethodCallIgnored
        assertThatThrownBy(() -> table.where("id", id).singleString("non_existing"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Column {non_existing} is not present");
    }

    @Test
    public void shouldThrowOnGetCountWithIllegalQuery() {
        //noinspection ResultOfMethodCallIgnored
        assertThatThrownBy(() -> table.where("non_existing_column", "10").getCount())
                .isInstanceOf(SQLException.class);
    }

    @Test
    public void shouldThrowIfSingleQueryReturnsMultipleRows() {
        table.insert().setField("code", 123).setField("name", "the same name").execute();
        table.insert().setField("code", 456).setField("name", "the same name").execute();

        //noinspection ResultOfMethodCallIgnored
        assertThatThrownBy(() -> table.where("name", "the same name").singleLong("code")).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void shouldRetrieveSavedInputStream() throws IOException {
        assumeLargeObjectsSupported();
        Object id = table.insert()
                .setPrimaryKey("id", null)
                .setField("code", 456)
                .setField("data", new ByteArrayInputStream("Hello World".getBytes()))
                .execute();
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        transfer(
                table.where("id", id)
                    .singleInputStream("data").orElseThrow(RuntimeException::new),
                buffer
        );
        assertThat(buffer.toString()).isEqualTo("Hello World");
    }
    
    @Test
    public void shouldRetrieveSavedReader() throws IOException {
        assumeLargeObjectsSupported();
        Object id = table.insert()
                .setPrimaryKey("id", null)
                .setField("code", 567)
                .setField("document", new StringReader("Hello World"))
                .execute();

        StringWriter buffer = new StringWriter();
        transfer(
                table.where("id", id)
                    .singleReader("document").orElseThrow(RuntimeException::new),
                buffer
        );
        assertThat(buffer.toString()).isEqualTo("Hello World");
    }

    public void insertTestRow(int code, String name) {
        table.insert().setFields(Arrays.asList("code", "name"), Arrays.asList(code, name))
                .execute();
    }

    private static void transfer(InputStream input, OutputStream output) throws IOException {
        byte[] buffer = new byte[8192];
        int read;
        while ((read = input.read(buffer, 0, 8192)) >= 0) {
            output.write(buffer, 0, read);
        }
    }
    
    private static void transfer(Reader input, Writer output) throws IOException {
        char[] buffer = new char[8192];
        int read;
        while ((read = input.read(buffer, 0, 8192)) >= 0) {
            output.write(buffer, 0, read);
        }
    }
}
