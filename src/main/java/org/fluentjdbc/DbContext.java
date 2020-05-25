package org.fluentjdbc;

import org.fluentjdbc.util.ExceptionUtil;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Optional;

public class DbContext {

    public DbTableContext table(DatabaseTable table) {
        return new DbTableContext(table, this);
    }

    public DbTableContext table(String tableName) {
        return table(new DatabaseTableImpl(tableName));
    }

    public DbTableContext tableWithTimestamps(String tableName) {
        return table(new DatabaseTableWithTimestamps(tableName));
    }

    public DbContextConnection startConnection(DataSource dataSource) {
        return startConnection(dataSource::getConnection);
    }

    public DbContextConnection startConnection(ConnectionSupplier connectionSupplier) {
        if (currentConnection.get() != null) {
            throw new IllegalStateException("Don't set twice in a thread!");
        }
        currentConnection.set(new DbContextConnection(connectionSupplier, this));
        currentCache.set(new HashMap<>());
        return currentConnection.get();
    }

    public Connection getThreadConnection() {
        if (currentConnection.get() == null) {
            throw new IllegalStateException("Call startConnection first");
        }
        return currentConnection.get().getConnection();
    }

    private static ThreadLocal<DbContextConnection> currentConnection = new ThreadLocal<>();
    private static ThreadLocal<HashMap<String, HashMap<Object, Optional<?>>>> currentCache = new ThreadLocal<>();

    void removeFromThread() {
        currentCache.get().clear();
        currentCache.remove();
        currentConnection.remove();
    }

    public static <ENTITY, KEY> Optional<ENTITY> cache(String tableName, KEY key, RetrieveMethod<KEY, ENTITY> retriever) {
        if (!currentCache.get().containsKey(tableName)) {
            currentCache.get().put(tableName, new HashMap<>());
        }
        if (!currentCache.get().get(tableName).containsKey(key)) {
            Optional<ENTITY> value = retriever.retrieve(key);
            currentCache.get().get(tableName).put(key, value);
        }
        return (Optional<ENTITY>) currentCache.get().get(tableName).get(key);
    }

    private ThreadLocal<DbTransaction> currentTransaction = new ThreadLocal<>();

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
        private DbTransaction outerTransaction;

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
}
