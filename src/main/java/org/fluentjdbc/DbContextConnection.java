package org.fluentjdbc;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.fluentjdbc.util.ExceptionUtil;

public class DbContextConnection implements AutoCloseable {

    private DataSource dataSource;
    private Connection connection;
    private DbContext context;

    public DbContextConnection(DataSource dataSource, DbContext context) {
        this.dataSource = dataSource;
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
                connection = dataSource.getConnection();
            } catch (SQLException e) {
                throw ExceptionUtil.softenCheckedException(e);
            }
        }
        return connection;
    }

}
