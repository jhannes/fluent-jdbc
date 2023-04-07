package org.fluentjdbc.junit5;

import org.fluentjdbc.DbContext;
import org.fluentjdbc.DbContextConnection;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import javax.sql.DataSource;

public class DbContextExtension extends DbContext implements BeforeEachCallback, AfterEachCallback {

    private final DataSource dataSource;

    public DbContextExtension(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void beforeEach(ExtensionContext context) {
        DbContextConnection connection = startConnection(dataSource);
        context.getStore(ExtensionContext.Namespace.GLOBAL)
                .put(DbContextConnection.class, connection);
    }

    @Override
    public void afterEach(ExtensionContext context) {
        DbContextConnection connection = context.getStore(ExtensionContext.Namespace.GLOBAL)
                .get(DbContextConnection.class, DbContextConnection.class);
        if (connection != null) {
            connection.close();
        }
    }
}
