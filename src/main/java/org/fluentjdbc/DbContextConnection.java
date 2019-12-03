package org.fluentjdbc;

import org.fluentjdbc.util.ExceptionUtil;

import java.sql.Connection;
import java.sql.SQLException;

public class DbContextConnection implements AutoCloseable {

    private ConnectionSupplier connectionSupplier;
    private Connection connection;
    private DbContext context;

    public DbContextConnection(ConnectionSupplier connectionSupplier, DbContext context) {
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

    public void commitTransaction() {
        if (connection != null) {
            try {
                connection.commit();
            } catch (SQLException e) {
                throw ExceptionUtil.softenCheckedException(e);
            }
        }
    }
}
