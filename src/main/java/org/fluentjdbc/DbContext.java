package org.fluentjdbc;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.HashMap;

public class DbContext {

    public DbTableContext table(String tableName) {
        return new DbTableContext(tableName, this);
    }

    public DbTableContext tableWithTimestamps(String tableName) {
        return new DbTableContext(new DatabaseTableWithTimestamps(tableName), this);
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

    Connection getThreadConnection() {
        if (currentConnection.get() == null) {
            throw new IllegalStateException("Call startConnection first");
        }
        return currentConnection.get().getConnection();
    }

    private static ThreadLocal<DbContextConnection> currentConnection = new ThreadLocal<>();
    private static ThreadLocal<HashMap<String, HashMap<Object, Object>>> currentCache = new ThreadLocal<>();

    void removeFromThread() {
        currentCache.get().clear();
        currentCache.remove();
        currentConnection.remove();
    }

    public static <ENTITY, KEY> ENTITY cache(String tableName, KEY key, RetrieveMethod<KEY, ENTITY> retriever) {
        if (!currentCache.get().containsKey(tableName)) {
            currentCache.get().put(tableName, new HashMap<>());
        }
        if (!currentCache.get().get(tableName).containsKey(key)) {
            currentCache.get().get(tableName).put(key, retriever.retrieve(key));
        }
        return (ENTITY) currentCache.get().get(tableName).get(key);
    }
}
