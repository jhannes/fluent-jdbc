package org.fluentjdbc;

import java.sql.Connection;
import javax.sql.DataSource;

public class DbContext {

    public DbTableContext table(String tableName) {
        return new DbTableContext(tableName, this);
    }

    public DbTableContext tableWithTimestamps(String tableName) {
        return new DbTableContext(new DatabaseTableWithTimestamps(tableName), this);
    }

    public DbContextConnection startConnection(DataSource dataSource) {
        if (currentConnection.get() != null) {
            throw new IllegalStateException("Don't set twice in a thread!");
        }
        currentConnection.set(new DbContextConnection(dataSource, this));
        return currentConnection.get();
    }

    Connection getThreadConnection() {
        if (currentConnection.get() == null) {
            throw new IllegalStateException("Call startConnection first");
        }
        return currentConnection.get().getConnection();
    }

    private static ThreadLocal<DbContextConnection> currentConnection = new ThreadLocal<>();

    void removeFromThread() {
        currentConnection.remove();
    }
}
