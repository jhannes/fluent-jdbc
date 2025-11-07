package org.fluentjdbc;

import org.fluentjdbc.util.ExceptionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;

/**
 * <p>Provides a starting point for for context oriented database operation. Create one DbContext for your
 * application and use {@link #table(String)} to create {@link DbContextTable} object for each table
 * you manipulate. All database operations must be nested inside a call to {@link #startConnection(DataSource)}.</p>
 *
 * <p>Example</p>
 * <pre>
 * DbContext context = new DbContext();
 *
 * {@link DbContextTable} table = context.table("database_test_table");
 * DataSource dataSource = createDataSource();
 *
 * try (DbContextConnection ignored = context.startConnection(dataSource)) {
 *     Object id = table.insert()
 *         .setPrimaryKey("id", null)
 *         .setField("code", 1002)
 *         .setField("name", "insertTest")
 *         .execute();
 *
 *     assertThat(table.where("name", "insertTest").orderBy("code").listLongs("code"))
 *         .contains(1002L);
 * }
 * </pre>
 *
 */
public class DbContext {
    
    private static final Logger logger = LoggerFactory.getLogger(DbContext.class);

    private final DatabaseStatementFactory factory;

    public DbContext() {
        this(new DatabaseStatementFactory(DatabaseReporter.LOGGING_REPORTER));
    }

    public DbContext(DatabaseStatementFactory factory) {
        this.factory = factory;
    }

    /**
     * Build an arbitrary select statement, e.g.
     * <code>dbContext.select("max(age) as max_age").from("persons").where("name", "Johannes").singleLong("max_age")</code>
     */
    @CheckReturnValue
    public DbContextSelectBuilder select(String... columns) {
        return new DbContextSelectBuilder(this).select(columns);
    }

    /**
     * Execute an arbitrary SQL statement, e.g.
     * <code>dbContext.select("select 1 as number from dual").singleLong("max_age")</code> or
     * <code>dbContext.select("update persons set full_name = first_name || ' ' || last_name").executeUpdate()</code>
     */
    @CheckReturnValue
    public DbContextStatement statement(String statement, Collection<Object> parameters) {
        return new DbContextStatement(this, statement, parameters);
    }

    public DatabaseStatementFactory getStatementFactory() {
        return factory;
    }

    /**
     * A {@link java.util.function.Supplier} for {@link Connection} objects. Like {@link java.util.function.Supplier},
     * but can throw {@link SQLException}. Used as an alternative to a {@link DataSource}
     */
    @FunctionalInterface
    public interface ConnectionSupplier {
        Connection getConnection() throws SQLException;
    }

    private final ThreadLocal<TopLevelDbContextConnection> currentConnection = new ThreadLocal<>();
    private final ThreadLocal<HashMap<String, HashMap<Object, SingleRow<?>>>> currentCache = new ThreadLocal<>();
    private final ThreadLocal<DbTransaction> currentTransaction = new ThreadLocal<>();

    /**
     * Creates a {@link DbContextTable} associated with this DbContext. All operations will be executed
     * with the connection from this {@link DbContext}
     */
    @CheckReturnValue
    public DbContextTable table(@Nonnull String tableName) {
        return table(new DatabaseTableImpl(tableName, factory));
    }

    /**
     * Creates a {@link #table(String)} which automatically updates <code>created_at</code> and
     * <code>updated_at</code> columns in the underlying database.
     *
     * @see DatabaseTableWithTimestamps
     */
    @CheckReturnValue
    public DbContextTable tableWithTimestamps(String tableName) {
        return table(new DatabaseTableWithTimestamps(tableName, factory));
    }

    /**
     * Associate a custom implementation of {@link DatabaseTable} with this {@link DbContext}
     */
    @CheckReturnValue
    public DbContextTable table(DatabaseTable table) {
        return new DbContextTable(table, this);
    }

    /**
     * Binds a database connection to this {@link DbContext} for the current thread. All database
     * operations on {@link DbContextTable} objects created from this {@link DbContext} in the current
     * thread will be executed using this connection. Calls to startConnection can be nested.
     *
     * <p>Example:</p>
     * <pre>
     * try (DbContextConnection ignored = context.startConnection(dataSource)) {
     *     return table.where("id", id).orderBy("data").listLongs("data");
     * }
     * </pre>
     */
    @CheckReturnValue
    public DbContextConnection startConnection(DataSource dataSource) {
        return startConnection(dataSource::getConnection);
    }

    /**
     * Gets a connection from {@link ConnectionSupplier} and assigns it to the current thread.
     * Associates the cache with the current thread as well
     *
     * @see #startConnection(DataSource)
     */
    @CheckReturnValue
    public DbContextConnection startConnection(ConnectionSupplier connectionSupplier) {
        if (currentConnection.get() != null) {
            return () -> { };
        }
        currentConnection.set(new TopLevelDbContextConnection(connectionSupplier, this));
        currentCache.set(new HashMap<>());
        return currentConnection.get();
    }

    /**
     * Returns the connection associated with the current thread or throws exception if
     * {@link #startConnection(DataSource)} has not been called yet
     */
    @CheckReturnValue
    public Connection getThreadConnection() {
        if (currentConnection.get() == null) {
            throw new IllegalStateException("Call startConnection first");
        }
        return currentConnection.get().getConnection();
    }

    void removeFromThread() {
        currentCache.get().clear();
        currentCache.remove();
        currentConnection.remove();
    }

    /**
     * Retrieves the underlying or cached value of the retriever argument. This cache is per
     * {@link DbContextConnection} and is evicted when the connection is closed
     */
    @CheckReturnValue
    public <ENTITY, KEY> SingleRow<ENTITY> cache(String tableName, KEY key, RetrieveMethod<KEY, ENTITY> retriever) {
        if (!currentCache.get().containsKey(tableName)) {
            currentCache.get().put(tableName, new HashMap<>());
        }
        if (!currentCache.get().get(tableName).containsKey(key)) {
            SingleRow<ENTITY> value = retriever.retrieve(key);
            currentCache.get().get(tableName).put(key, value);
        }
        //noinspection unchecked
        return (SingleRow<ENTITY>) currentCache.get().get(tableName).get(key);
    }

    /**
     * Turns off auto-commit for the current thread until the {@link DbTransaction} is closed. Returns
     * a {@link DbTransaction} object which can be used to control commit and rollback. Can be nested
     */
    @CheckReturnValue
    public DbTransaction ensureTransaction() {
        if (getCurrentTransaction() != null) {
            logger.debug("Starting nested transaction");
            return new NestedTransactionContext(getCurrentTransaction());
        }
        logger.debug("Starting new transaction");
        currentTransaction.set(new TopLevelTransaction(getThreadConnection()));
        return getCurrentTransaction();
    }

    public DbTransaction getCurrentTransaction() {
        return currentTransaction.get();
    }

    private static class NestedTransactionContext implements DbTransaction {
        private boolean complete = false;
        private final DbTransaction outerTransaction;

        public NestedTransactionContext(DbTransaction outerTransaction) {
            this.outerTransaction = outerTransaction;
        }

        @Override
        public void close() {
            if (!complete) {
                outerTransaction.setRollback();
                logger.debug("Nested transaction rollback");
            } else {
                logger.debug("Nested transaction commit");
            }
        }

        @Override
        public void setRollback() {
            complete = false;
        }

        @Override
        public void setComplete() {
            complete = true;
        }
    }

    private class TopLevelTransaction implements DbTransaction {
        private final boolean autoCommit;
        boolean complete = false;
        boolean rollback = false;

        private TopLevelTransaction(Connection connection) {
            try {
                this.autoCommit = connection.getAutoCommit();
                getThreadConnection().setAutoCommit(false);
            } catch (SQLException e) {
                throw ExceptionUtil.softenCheckedException(e);
            }
        }

        @Override
        public void setComplete() {
            complete = true;
        }

        @Override
        public void setRollback() {
            rollback = true;
        }

        @Override
        public void close() {
            currentTransaction.remove();
            try {
                if (!complete || rollback) {
                    logger.debug("Rollback");
                    getThreadConnection().rollback();
                } else {
                    logger.debug("Commit");
                    getThreadConnection().commit();
                }
                getThreadConnection().setAutoCommit(autoCommit);
            } catch (SQLException e) {
                throw ExceptionUtil.softenCheckedException(e);
            }
        }
    }

    static class TopLevelDbContextConnection implements DbContextConnection {

        private final ConnectionSupplier connectionSupplier;
        private Connection connection;
        private final DbContext context;

        TopLevelDbContextConnection(ConnectionSupplier connectionSupplier, DbContext context) {
            this.connectionSupplier = connectionSupplier;
            this.context = context;
        }

        @Override
        public void close() {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    throw ExceptionUtil.softenCheckedException(e);
                }
            }
            context.removeFromThread();
        }

        Connection getConnection() {
            if (connection == null) {
                try {
                    connection = connectionSupplier.getConnection();
                } catch (SQLException e) {
                    throw ExceptionUtil.softenCheckedException(e);
                }
            }
            return connection;
        }

    }

    /**
     * Functional interface used to populate the query. Called on when a retrieved value is not in
     * the cache. Like {@link java.util.function.Function}, but returns {@link SingleRow}
     */
    @FunctionalInterface
    public interface RetrieveMethod<KEY, ENTITY> {
        SingleRow<ENTITY> retrieve(KEY key);
    }
}
