package org.fluentjdbc;

import static org.assertj.core.api.Assertions.assertThat;

import org.fluentjdbc.DatabaseTable.RowMapper;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class SynchronizeDataTest {

    private static final String CREATE_TABLE =
            "create table demo_table (id integer primary key auto_increment, name varchar not null, updated_at datetime not null, created_at datetime not null)";

    private Connection clientConnection;
    private Connection serverConnection;

    private DatabaseTable table = new DatabaseTableWithTimestamps("demo_table");

    @Before
    public void openConnection() throws SQLException {
        JdbcDataSource serverDataSource = new JdbcDataSource();
        serverDataSource.setUrl("jdbc:h2:mem:" + getClass().getName() + "-server");
        serverConnection = serverDataSource.getConnection();

        try(Statement stmt = serverConnection.createStatement()) {
            stmt.executeUpdate(CREATE_TABLE);
        }


        JdbcDataSource clientDataSource = new JdbcDataSource();
        clientDataSource.setUrl("jdbc:h2:mem:" + getClass().getName());
        clientConnection = clientDataSource.getConnection();

        try(Statement stmt = clientConnection.createStatement()) {
            stmt.executeUpdate(CREATE_TABLE);
        }
    }

    @After
    public void closeConnections() throws SQLException {
        clientConnection.close();
        serverConnection.close();
    }

    @Test
    public void shouldDownloadChanges() throws Exception {
        long id = table.newSaveBuilder("id", null).setField("name", "some name").execute(serverConnection);

        assertThat(table.where("id", id).singleString(clientConnection, "name")).isNull();

        synchronize(serverConnection, clientConnection);

        assertThat(table.where("id", id).singleString(clientConnection, "name")).isEqualTo("some name");
    }

    private void synchronize(Connection serverConnection, Connection clientConnection) {
        List<String> names = table.listObjects(serverConnection, new RowMapper<String>() {
            @Override
            public String mapRow(DatabaseRow row) throws SQLException {
                return row.getString("name");
            }
        });

        for (String name : names) {
            table.newSaveBuilder("id", null).setField("name", name).execute(clientConnection);
        }
    }

}
