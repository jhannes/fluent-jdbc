package org.fluentjdbc;

import org.fluentjdbc.util.ExceptionUtil;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Optional;

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

    private final DatabaseReporter reporter;

    public DbContext() {
        this(DatabaseReporter.LOGGING_REPORTER);
    }

    public DbContext(DatabaseReporter reporter) {
        this.reporter = reporter;
    }

    public DatabaseTableReporter tableReporter(DatabaseTable table) {
        return tableReporter(table.getTableName());
    }

    public DatabaseTableReporter tableReporter(String tableName) {
        return reporter.table(tableName);
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
    private final ThreadLocal<HashMap<String, HashMap<Object, Optional<?>>>> currentCache = new ThreadLocal<>();
    private final ThreadLocal<DbTransaction> currentTransaction = new ThreadLocal<>();

    /**
     * Creates a {@link DbContextTable} associated with this DbContext. All operations will be executed
     * with the connection from this {@link DbContext}
     */
    public DbContextTable table(String tableName) {
        return table(new DatabaseTableImpl(tableName, tableReporter(tableName)));
    }

    /**
     * Creates a {@link #table(String)} which automatically updates <code>created_at</code> and
     * <code>updated_at</code> columns in the underlying database.
     *
     * @see DatabaseTableWithTimestamps
     */
    public DbContextTable tableWithTimestamps(String tableName) {
        return table(new DatabaseTableWithTimestamps(tableName, tableReporter(tableName)));
    }

    /**
     * Associate a custom implementation of {@link DatabaseTable} with this {@link DbContext}
     */
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
    public DbContextConnection startConnection(DataSource dataSource) {
        return startConnection(dataSource::getConnection);
    }

    /**
     * Gets a connection from {@link ConnectionSupplier} and assigns it to the the current thread.
     * Associates the cache with the current thread as well
     * 
     * @see #startConnection(DataSource)
     */
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
    public <ENTITY, KEY> Optional<ENTITY> cache(String tableName, KEY key, RetrieveMethod<KEY, ENTITY> retriever) {
        if (!currentCache.get().containsKey(tableName)) {
            currentCache.get().put(tableName, new HashMap<>());
        }
        if (!currentCache.get().get(tableName).containsKey(key)) {
            Optional<ENTITY> value = retriever.retrieve(key);
            currentCache.get().get(tableName).put(key, value);
        }
        return (Optional<ENTITY>) currentCache.get().get(tableName).get(key);
    }

    /**
     * Turns off auto-commit for the current thread until the {@link DbTransaction} is closed. Returns
     * a {@link DbTransaction} object which can be used to control commit and rollback. Can be nested
     */
    public DbTransaction ensureTransaction() {
        if (currentTransaction.get() != null) {
            return new NestedTransactionContext(currentTransaction.get());
        }
        try {
            getThreadConnection().setAutoCommit(false);
        } catch (SQLException e) {
            throw ExceptionUtil.softenCheckedException(e);
        }
        currentTransaction.set(new TopLevelTransaction());
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
        boolean complete = false;
        boolean rollback = false;

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
                    getThreadConnection().rollback();
                } else {
                    getThreadConnection().commit();
                }
                getThreadConnection().setAutoCommit(false);
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
     * the cache. Like {@link java.util.function.Function}, but returns {@link Optional}
     */
    @FunctionalInterface
    public interface RetrieveMethod<KEY, ENTITY> {
        Optional<ENTITY> retrieve(KEY key);
    }
}
